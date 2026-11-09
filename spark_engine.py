"""Extract, transform, and load epidemiological data with PySpark."""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any

import requests
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

from db_config import DatabaseConfig

logger = logging.getLogger(__name__)

DATASET_URL = (
    "https://raw.githubusercontent.com/rs-delve/covid19_datasets/master/"
    "dataset/combined_dataset_latest.csv"
)
DATASET_REPOSITORY_URL = "https://github.com/rs-delve/covid19_datasets"
TARGET_TABLE = "seguimiento_epidemiologico"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"
PREVIEW_LIMIT = 8

# Canonical ETL names on the left; corresponding columns in the DELVE CSV on the right.
SOURCE_COLUMN_ALIASES = {
    "iso_code": "ISO",
    "location": "country_name",
    "date": "DATE",
    "total_cases": "cases_total",
    "new_cases": "cases_new",
    "new_cases_smoothed": "cases_new_smoothed",
    "total_deaths": "deaths_total",
    "new_deaths": "deaths_new",
    "new_deaths_smoothed": "deaths_new_smoothed",
    "reproduction_rate": "reproduction_rate",
    "icu_patients": "icu_patients",
    "hosp_patients": "hosp_patients",
    "total_vaccinations": "vaccines_total",
    "people_vaccinated": "vaccines_people_vaccinated",
    "people_fully_vaccinated": "vaccines_people_fully_vaccinated",
    "population": "stats_population",
    "population_density": "stats_population_density",
    "median_age": "stats_median_age",
    "aged_65_older": "stats_population_65plus",
    "aged_70_older": "stats_population_70plus",
    "gdp_per_capita": "stats_gdp_per_capita",
    "life_expectancy": "stats_life_expectancy",
    "cardiovasc_death_rate": "stats_cardiovasc_death_rate",
    "diabetes_prevalence": "stats_diabetes_prevalence",
}

SELECTED_COLUMNS = (
    "iso_code",
    "continent",
    "location",
    "date",
    "total_cases",
    "new_cases",
    "new_cases_smoothed",
    "total_deaths",
    "new_deaths",
    "new_deaths_smoothed",
    "reproduction_rate",
    "icu_patients",
    "hosp_patients",
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "population",
    "population_density",
    "median_age",
    "aged_65_older",
    "aged_70_older",
    "gdp_per_capita",
    "life_expectancy",
    "cardiovasc_death_rate",
    "diabetes_prevalence",
)
NUMERIC_COLUMNS = tuple(column for column in SELECTED_COLUMNS if column not in {
    "iso_code",
    "continent",
    "location",
    "date",
})
DAILY_METRICS = ("new_cases", "new_cases_smoothed", "new_deaths", "new_deaths_smoothed")
PREVIEW_COLUMNS = (
    "location",
    "date",
    "total_cases",
    "new_cases",
    "total_deaths",
    "letalidad_pct",
    "promedio_movil_7d",
    "grupo_etario_pais",
)
MYSQL_TYPE_BY_SPARK_TYPE = {
    BooleanType: "BOOLEAN",
    ByteType: "TINYINT",
    DateType: "DATE",
    DecimalType: "DECIMAL(38, 18)",
    DoubleType: "DOUBLE",
    FloatType: "DOUBLE",
    IntegerType: "INT",
    LongType: "BIGINT",
    ShortType: "SMALLINT",
    TimestampType: "DATETIME(6)",
}

_lock = threading.RLock()
_spark: SparkSession | None = None
_raw_df: DataFrame | None = None
_processed_df: DataFrame | None = None
_download_directory: Path | None = None
_source_stats: dict[str, int] | None = None
_etl_metrics: dict[str, int] | None = None


class SparkEngineError(RuntimeError):
    """Raised when an ETL operation cannot be completed."""


def get_spark_session() -> SparkSession:
    """Return the process-wide Spark session, creating it only once."""
    global _spark
    with _lock:
        if _spark is None:
            try:
                ivy_directory = Path(
                    os.getenv(
                        "SPARK_IVY_DIR",
                        str(Path(tempfile.gettempdir()) / "spark-ivy"),
                    )
                ).expanduser().resolve()
                ivy_directory.mkdir(parents=True, exist_ok=True)
                master = os.getenv("SPARK_MASTER", "local[*]")
                _spark = (
                    SparkSession.builder.appName("seguimiento-epidemiologico")
                    .master(master)
                    .config("spark.jars.packages", JDBC_PACKAGE)
                    .config("spark.jars.ivy", str(ivy_directory))
                    .config("spark.sql.session.timeZone", "UTC")
                )
                if master.startswith("local"):
                    _spark = (
                        _spark.config(
                            "spark.driver.host",
                            os.getenv("SPARK_DRIVER_HOST", "127.0.0.1"),
                        )
                        .config(
                            "spark.driver.bindAddress",
                            os.getenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1"),
                        )
                    )
                _spark = _spark.getOrCreate()
                _spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
            except Exception as exc:
                logger.exception("No se pudo iniciar Spark")
                raise SparkEngineError(
                    "No se pudo iniciar PySpark. Verifica Java y la configuración del servicio."
                ) from exc
        return _spark


def _preview(frame: DataFrame, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    """Convert a small Spark sample to JSON-compatible dictionaries."""
    return [json.loads(row) for row in frame.select(*columns).limit(PREVIEW_LIMIT).toJSON().collect()]


def extraer_dataset() -> dict[str, Any]:
    """Download the RS-DELVE CSV, read it with Spark, and return source statistics."""
    global _raw_df, _processed_df, _download_directory, _source_stats, _etl_metrics
    with _lock:
        spark = get_spark_session()
        temporary_directory: Path | None = None
        frame: DataFrame | None = None
        try:
            temporary_directory = Path(tempfile.mkdtemp(prefix="epidemiologia-"))
            csv_path = temporary_directory / "rs-delve-covid19.csv"
            with requests.get(DATASET_URL, stream=True, timeout=(15, 180)) as response:
                response.raise_for_status()
                with csv_path.open("wb") as csv_file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            csv_file.write(chunk)

            frame = (
                spark.read.option("header", True)
                .option("inferSchema", False)
                .option("mode", "FAILFAST")
                .csv(str(csv_path))
            )
            frame.persist(StorageLevel.MEMORY_AND_DISK)
            row_count = frame.count()
            preview_columns = (
                "ISO",
                "country_name",
                "DATE",
                "cases_total",
                "cases_new",
                "deaths_total",
                "stats_population",
            )
            preview = _preview(frame, preview_columns)
        except Exception as exc:
            if frame is not None:
                frame.unpersist()
            if temporary_directory is not None:
                shutil.rmtree(temporary_directory, ignore_errors=True)
            logger.exception("No se pudo extraer el dataset de RS-DELVE")
            raise SparkEngineError(
                "No fue posible descargar o leer el CSV de RS-DELVE. Intenta nuevamente."
            ) from exc

        if _raw_df is not None:
            _raw_df.unpersist()
        if _processed_df is not None:
            _processed_df.unpersist()
        if _download_directory is not None:
            shutil.rmtree(_download_directory, ignore_errors=True)
        _raw_df = frame
        _processed_df = None
        _download_directory = temporary_directory
        _etl_metrics = None
        _source_stats = {"rows": row_count, "columns": len(frame.columns)}
        return {
            **_source_stats,
            "source_url": DATASET_URL,
            "source_repository": DATASET_REPOSITORY_URL,
            "preview": preview,
            "preview_columns": list(preview_columns),
        }


def _normalize_source_schema(frame: DataFrame) -> DataFrame:
    """Rename DELVE source columns to the canonical names used by the ETL."""
    for canonical, source_column in SOURCE_COLUMN_ALIASES.items():
        if canonical not in frame.columns and source_column in frame.columns:
            frame = frame.withColumnRenamed(source_column, canonical)
    return frame


def _add_missing_columns(frame: DataFrame) -> DataFrame:
    """Keep the transform tolerant of upstream CSV schema additions/removals."""
    for column in SELECTED_COLUMNS:
        if column not in frame.columns:
            data_type = StringType() if column in {"iso_code", "continent", "location", "date"} else DoubleType()
            frame = frame.withColumn(column, F.lit(None).cast(data_type))
    return frame.select(*SELECTED_COLUMNS)


def transformar_datos_pyspark() -> dict[str, Any]:
    """Normalize RS-DELVE data, derive epidemiological KPIs, and report cleaning counts."""
    global _processed_df, _etl_metrics
    with _lock:
        if _raw_df is None:
            raise SparkEngineError("Primero descarga el dataset crudo.")

        processed: DataFrame | None = None
        try:
            selected = _add_missing_columns(_normalize_source_schema(_raw_df))
            typed = selected.withColumn(
                "location",
                F.lower(F.regexp_replace(F.trim(F.col("location")), r"\s+", " ")),
            ).withColumn(
                "date",
                F.try_to_timestamp(
                    F.col("date").cast("string"),
                    F.lit("yyyy-MM-dd"),
                ).cast("date"),
            )

            for column in NUMERIC_COLUMNS:
                typed = typed.withColumn(column, F.col(column).cast(DoubleType()))

            null_key_condition = (
                F.col("iso_code").isNull()
                | (F.length(F.trim(F.col("iso_code"))) == 0)
                | F.col("location").isNull()
                | (F.length(F.trim(F.col("location"))) == 0)
                | F.col("date").isNull()
            )
            removed_null_keys = typed.filter(null_key_condition).count()
            keyed = typed.filter(~null_key_condition)

            aggregate_condition = F.col("iso_code").startswith("OWID")
            removed_aggregates = keyed.filter(aggregate_condition).count()
            countries = keyed.filter(~aggregate_condition)
            before_deduplication = countries.count()
            deduplicated = countries.dropDuplicates(["location", "date"])
            after_deduplication = deduplicated.count()
            removed_duplicates = before_deduplication - after_deduplication

            imputation_counts = deduplicated.agg(
                *(
                    F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
                    for column in DAILY_METRICS
                )
            ).first()
            imputed_daily_values = sum(
                int(imputation_counts[column] or 0) for column in DAILY_METRICS
            )
            filled = deduplicated.fillna(0, subset=list(DAILY_METRICS))
            moving_average = Window.partitionBy("location").orderBy("date").rowsBetween(-6, 0)

            processed = (
                filled.withColumn(
                    "letalidad_pct",
                    F.when(
                        F.col("total_cases").isNotNull() & (F.col("total_cases") > 0),
                        (F.col("total_deaths") / F.col("total_cases")) * F.lit(100.0),
                    ).otherwise(F.lit(0.0)),
                )
                .withColumn(
                    "promedio_movil_7d",
                    F.avg("new_cases").over(moving_average),
                )
                .withColumn(
                    "grupo_etario_pais",
                    F.when(F.col("median_age") < 25, "Joven")
                    .when(F.col("median_age") < 38, "Intermedia")
                    .when(F.col("median_age") >= 38, "Envejecida")
                    .otherwise("No disponible"),
                )
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            row_count = processed.count()
            preview = _preview(processed, PREVIEW_COLUMNS)
        except SparkEngineError:
            raise
        except Exception as exc:
            if processed is not None:
                processed.unpersist()
            logger.exception("Falló la transformación de datos con PySpark")
            raise SparkEngineError(
                "Falló la transformación del dataset. Revisa los logs del servidor."
            ) from exc

        if _processed_df is not None:
            _processed_df.unpersist()
        _processed_df = processed
        _etl_metrics = {
            "removed_null_keys": removed_null_keys,
            "removed_aggregates": removed_aggregates,
            "removed_duplicates": removed_duplicates,
            "imputed_daily_values": int(imputed_daily_values),
        }
        return {
            "rows": row_count,
            "columns": len(processed.columns),
            **_etl_metrics,
            "preview": preview,
            "preview_columns": list(PREVIEW_COLUMNS),
        }


def _mysql_staging_table_ddl(frame: DataFrame, table_name: str) -> str:
    """Create MySQL DDL with a composite primary key for a processed frame."""
    if not table_name.replace("_", "").isalnum():
        raise SparkEngineError("El nombre de la tabla temporal no es válido.")

    available_columns = set(frame.columns)
    if not {"location", "date"}.issubset(available_columns):
        raise SparkEngineError(
            "El dataset transformado no contiene las columnas de clave primaria."
        )

    column_definitions = []
    for field in frame.schema.fields:
        column_name = field.name
        if not column_name.replace("_", "").isalnum():
            raise SparkEngineError("El esquema contiene un nombre de columna inválido.")
        if column_name == "location":
            mysql_type = "VARCHAR(191)"
        elif isinstance(field.dataType, StringType):
            mysql_type = "VARCHAR(255)"
        else:
            mysql_type = next(
                (
                    sql_type
                    for spark_type, sql_type in MYSQL_TYPE_BY_SPARK_TYPE.items()
                    if isinstance(field.dataType, spark_type)
                ),
                None,
            )
            if mysql_type is None:
                raise SparkEngineError(
                    f"Tipo de dato no soportado para MySQL: {field.dataType.simpleString()}."
                )

        not_null = " NOT NULL" if column_name in {"location", "date"} else ""
        column_definitions.append(f"`{column_name}` {mysql_type}{not_null}")

    column_definitions.append("PRIMARY KEY (`location`, `date`)")
    return (
        f"CREATE TABLE `{table_name}` ({', '.join(column_definitions)}) "
        "ENGINE=InnoDB"
    )


def cargar_a_mysql(db_config: DatabaseConfig) -> dict[str, Any]:
    """Atomically replace the MySQL table with a PK-constrained Spark load."""
    with _lock:
        if _processed_df is None:
            raise SparkEngineError("Primero ejecuta la transformación ETL.")

        spark = get_spark_session()
        jdbc_url = (
            f"jdbc:mysql://{db_config.host}:{db_config.port}/{db_config.database}"
            f"?sslMode={db_config.jdbc_ssl_mode}"
        )
        suffix = uuid.uuid4().hex[:12]
        staging_table = f"{TARGET_TABLE}_stage_{suffix}"
        backup_table = f"{TARGET_TABLE}_backup_{suffix}"
        quoted_target = f"`{TARGET_TABLE}`"
        quoted_staging = f"`{staging_table}`"
        quoted_backup = f"`{backup_table}`"
        connection = None
        statement = None
        staging_created = False
        swap_completed = False

        try:
            create_table_sql = _mysql_staging_table_ddl(
                _processed_df,
                staging_table,
            )
            connection = spark._jvm.java.sql.DriverManager.getConnection(
                jdbc_url,
                db_config.username,
                db_config.password,
            )
            statement = connection.createStatement()
            statement.execute(create_table_sql)
            staging_created = True

            (
                _processed_df.write.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", staging_table)
                .option("user", db_config.username)
                .option("password", db_config.password)
                .option("driver", JDBC_DRIVER)
                .mode("append")
                .save()
            )

            table_check = connection.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?"
            )
            try:
                table_check.setString(1, db_config.database)
                table_check.setString(2, TARGET_TABLE)
                result_set = table_check.executeQuery()
                try:
                    target_exists = result_set.next() and result_set.getInt(1) > 0
                finally:
                    result_set.close()
            finally:
                table_check.close()

            if target_exists:
                statement.execute(
                    f"RENAME TABLE {quoted_target} TO {quoted_backup}, "
                    f"{quoted_staging} TO {quoted_target}"
                )
                swap_completed = True
                try:
                    statement.execute(f"DROP TABLE {quoted_backup}")
                except Exception:
                    logger.warning(
                        "La carga se completó, pero no se pudo eliminar la tabla de respaldo %s",
                        backup_table,
                        exc_info=True,
                    )
            else:
                statement.execute(
                    f"RENAME TABLE {quoted_staging} TO {quoted_target}"
                )
                swap_completed = True
        except Exception as exc:
            if isinstance(exc, SparkEngineError):
                raise
            logger.exception("No se pudo cargar el dataset en MySQL")
            raise SparkEngineError(
                "No fue posible cargar los datos en MySQL. Verifica la conexión, credenciales y permisos."
            ) from exc
        finally:
            if connection is not None:
                if staging_created and not swap_completed:
                    try:
                        cleanup_statement = connection.createStatement()
                        try:
                            cleanup_statement.execute(f"DROP TABLE IF EXISTS {quoted_staging}")
                        finally:
                            cleanup_statement.close()
                    except Exception:
                        logger.warning(
                            "No se pudo limpiar la tabla temporal %s",
                            staging_table,
                            exc_info=True,
                        )
                if statement is not None:
                    statement.close()
                connection.close()

        return {
            "table": TARGET_TABLE,
            "rows": _processed_df.count(),
            "database": db_config.database,
            "target": db_config.target,
        }


def close_spark_session() -> None:
    """Release cached DataFrames, downloaded data, and the Spark session."""
    global _spark, _raw_df, _processed_df, _download_directory, _source_stats, _etl_metrics
    with _lock:
        if _raw_df is not None:
            _raw_df.unpersist()
        if _processed_df is not None:
            _processed_df.unpersist()
        if _spark is not None:
            _spark.stop()
        if _download_directory is not None:
            shutil.rmtree(_download_directory, ignore_errors=True)
        _spark = None
        _raw_df = None
        _processed_df = None
        _download_directory = None
        _source_stats = None
        _etl_metrics = None
