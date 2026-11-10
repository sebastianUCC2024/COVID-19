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
from py4j.protocol import Py4JNetworkError
from pyspark import SparkContext, StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
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

from sqlalchemy import text

from db_config import DatabaseConfig, create_sqlalchemy_engine

logger = logging.getLogger(__name__)

DATASET_URL = (
    "https://raw.githubusercontent.com/owid/covid-19-data/master/"
    "public/data/owid-covid-data.csv"
)
DATASET_REPOSITORY_URL = "https://github.com/owid/covid-19-data"
TARGET_TABLE = "seguimiento_epidemiologico"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"
PREVIEW_LIMIT = 8

# Canonical OWID columns, with legacy DELVE aliases retained for compatibility.
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
PREVIEW_COLUMNS = (
    "location",
    "date",
    "total_cases",
    "new_cases",
    "total_deaths",
    "tasa_letalidad",
    "casos_promedio_7d",
    "grupo_etario_pais",
)
IMPUTED_COLUMNS = ("total_cases", "total_deaths", "new_cases", "new_deaths")
DERIVED_COLUMNS = ("tasa_letalidad", "casos_promedio_7d", "grupo_etario_pais")
MYSQL_STRING_COLUMNS = {
    "iso_code": "VARCHAR(16)",
    "continent": "VARCHAR(100)",
    "location": "VARCHAR(191)",
    "grupo_etario_pais": "VARCHAR(50)",
}
MYSQL_OUTPUT_COLUMNS = (
    *SELECTED_COLUMNS,
    *DERIVED_COLUMNS,
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


def _spark_gateway_is_alive() -> bool:
    """Check that the Py4J gateway process and JVM are still reachable."""
    if _spark is None:
        return False

    gateway = SparkContext._gateway
    process = getattr(gateway, "proc", None)
    if process is not None:
        exit_code = process.poll()
        if exit_code is not None:
            logger.error("El proceso JVM de Spark terminó con código %s", exit_code)
            return False

    try:
        _spark.sparkContext._jvm.java.lang.System.currentTimeMillis()
    except (OSError, Py4JNetworkError):
        logger.exception("El gateway JVM de Spark no responde")
        return False
    return True


def _discard_spark_state() -> None:
    """Forget Spark objects whose JVM has exited without calling the dead gateway."""
    global _spark, _raw_df, _processed_df, _download_directory, _source_stats, _etl_metrics
    _spark = None
    _raw_df = None
    _processed_df = None
    _source_stats = None
    _etl_metrics = None
    if _download_directory is not None:
        shutil.rmtree(_download_directory, ignore_errors=True)
    _download_directory = None
    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None
    SparkContext._active_spark_context = None
    SparkContext._gateway = None
    SparkContext._jvm = None


def get_spark_session() -> SparkSession:
    """Return the process-wide Spark session, creating it only once."""
    global _spark
    with _lock:
        if _spark is not None and not _spark_gateway_is_alive():
            _discard_spark_state()
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
                    .config(
                        "spark.sql.shuffle.partitions",
                        os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
                    )
                )
                if master.startswith("local"):
                    driver_host = os.getenv("SPARK_DRIVER_HOST")
                    driver_bind_address = os.getenv("SPARK_DRIVER_BIND_ADDRESS")
                    if driver_host:
                        _spark = _spark.config("spark.driver.host", driver_host)
                    if driver_bind_address:
                        _spark = _spark.config(
                            "spark.driver.bindAddress",
                            driver_bind_address,
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
    """Download the OWID CSV, read it with Spark, and return source statistics."""
    global _raw_df, _processed_df, _download_directory, _source_stats, _etl_metrics
    with _lock:
        spark = get_spark_session()
        temporary_directory: Path | None = None
        frame: DataFrame | None = None
        try:
            temporary_directory = Path(tempfile.mkdtemp(prefix="epidemiologia-"))
            csv_path = temporary_directory / "owid-covid-data.csv"
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
                "iso_code",
                "location",
                "date",
                "total_cases",
                "new_cases",
                "total_deaths",
                "population",
            )
            preview = _preview(frame, preview_columns)
        except Exception as exc:
            if frame is not None:
                frame.unpersist()
            if temporary_directory is not None:
                shutil.rmtree(temporary_directory, ignore_errors=True)
            logger.exception("No se pudo extraer el dataset de OWID")
            raise SparkEngineError(
                "No fue posible descargar o leer el CSV de OWID. Intenta nuevamente."
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
    """Accept canonical OWID names and legacy DELVE aliases."""
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


def _clean_text_column(column: str, lowercase: bool = False) -> Column:
    """Normalize whitespace and remove punctuation noise from a text field."""
    cleaned = F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.col(column),
                r"[^\p{L}\p{N}\s'’.-]",
                " ",
            ),
            r"\s+",
            " ",
        )
    )
    if lowercase:
        cleaned = F.lower(cleaned)
    return F.when(F.length(cleaned) > 0, cleaned).otherwise(
        F.lit(None).cast(StringType())
    )


def transformar_datos_pyspark() -> dict[str, Any]:
    """Clean OWID observations, derive KPIs, and report cleaning counts."""
    global _processed_df, _etl_metrics
    with _lock:
        if _raw_df is None:
            raise SparkEngineError("Primero descarga el dataset crudo.")
        if not _spark_gateway_is_alive():
            _discard_spark_state()
            raise SparkEngineError(
                "La JVM de Spark se cerró y el dataset en memoria ya no está disponible. "
                "Vuelve a ejecutar la extracción."
            )

        processed: DataFrame | None = None
        cached_intermediates: list[DataFrame] = []
        try:
            selected = _add_missing_columns(_normalize_source_schema(_raw_df))
            typed = selected.withColumn(
                "location", _clean_text_column("location", lowercase=True)
            ).withColumn(
                "continent", _clean_text_column("continent")
            ).withColumn(
                "iso_code",
                F.upper(F.trim(F.col("iso_code"))),
            ).withColumn(
                "date",
                F.try_to_timestamp(
                    F.col("date").cast("string"),
                    F.lit("yyyy-MM-dd"),
                ).cast("date"),
            )

            for column in NUMERIC_COLUMNS:
                cast_value = F.col(column).cast(DoubleType())
                invalid_number = F.isnan(cast_value) | (
                    F.abs(cast_value) == F.lit(float("inf"))
                )
                typed = typed.withColumn(
                    column,
                    F.when(invalid_number, F.lit(None).cast(DoubleType())).otherwise(
                        cast_value
                    ),
                )

            typed = typed.persist(StorageLevel.MEMORY_AND_DISK)
            cached_intermediates.append(typed)
            null_key_condition = (
                F.col("iso_code").isNull()
                | (F.length(F.trim(F.col("iso_code"))) == 0)
                | F.col("location").isNull()
                | (F.length(F.trim(F.col("location"))) == 0)
                | F.col("date").isNull()
            )
            null_key_stats = typed.agg(
                F.sum(F.when(null_key_condition, 1).otherwise(0)).alias("removed")
            ).first()
            removed_null_keys = int(null_key_stats["removed"] or 0)
            keyed = typed.filter(~null_key_condition)

            aggregate_condition = F.col("iso_code").startswith("OWID_")
            aggregate_stats = keyed.agg(
                F.sum(F.when(aggregate_condition, 1).otherwise(0)).alias("removed"),
                F.sum(F.when(~aggregate_condition, 1).otherwise(0)).alias("countries"),
            ).first()
            removed_aggregates = int(aggregate_stats["removed"] or 0)
            countries = keyed.filter(~aggregate_condition)
            before_deduplication = int(aggregate_stats["countries"] or 0)
            deduplicated = countries.dropDuplicates(["location", "date"])
            deduplicated = deduplicated.persist(StorageLevel.MEMORY_AND_DISK)
            cached_intermediates.append(deduplicated)
            deduplicated_stats = deduplicated.agg(
                F.count("*").alias("rows"),
                *(
                    F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
                    for column in IMPUTED_COLUMNS
                ),
            ).first()
            after_deduplication = int(deduplicated_stats["rows"] or 0)
            removed_duplicates = before_deduplication - after_deduplication
            imputed_daily_values = sum(
                int(deduplicated_stats[column] or 0) for column in IMPUTED_COLUMNS
            )
            filled = deduplicated.fillna(0, subset=list(IMPUTED_COLUMNS))
            first_case = Window.partitionBy("location")
            with_first_case = filled.withColumn(
                "_first_case_date",
                F.min(
                    F.when(F.col("total_cases") > 0, F.col("date"))
                ).over(first_case),
            )
            active_countries = with_first_case.filter(
                F.col("_first_case_date").isNotNull()
                & (F.col("date") >= F.col("_first_case_date"))
            ).drop("_first_case_date")
            moving_average = (
                Window.partitionBy("location")
                .orderBy(F.col("date").cast("timestamp").cast("long"))
                .rangeBetween(-6 * 24 * 60 * 60, 0)
            )

            processed = (
                active_countries.withColumn(
                    "tasa_letalidad",
                    F.when(
                        F.col("total_cases") > 0,
                        (F.col("total_deaths") / F.col("total_cases")) * F.lit(100.0),
                    ).otherwise(F.lit(0.0)),
                )
                .withColumn(
                    "casos_promedio_7d",
                    F.avg("new_cases").over(moving_average),
                )
                .withColumn(
                    "grupo_etario_pais",
                    F.when(F.col("aged_65_older").isNull(), "No disponible")
                    .when(F.col("aged_65_older") >= 15, "Población Envejecida")
                    .when(F.col("aged_65_older") >= 8, "Población Intermedia")
                    .otherwise("Población Joven"),
                )
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            row_count = processed.count()
            removed_pre_first_case = after_deduplication - row_count
            preview = _preview(processed, PREVIEW_COLUMNS)
            for frame in cached_intermediates:
                frame.unpersist()
        except SparkEngineError:
            for frame in cached_intermediates:
                frame.unpersist()
            if processed is not None:
                processed.unpersist()
            raise
        except Exception as exc:
            for frame in cached_intermediates:
                frame.unpersist()
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
            "removed_pre_first_case": removed_pre_first_case,
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


def _mysql_empty_table_ddl(table_name: str) -> str:
    """Create the optimized empty table schema expected by the OWID ETL."""
    if not table_name.replace("_", "").isalnum():
        raise SparkEngineError("El nombre de la tabla no es válido.")

    column_definitions = []
    for column_name in MYSQL_OUTPUT_COLUMNS:
        if not column_name.replace("_", "").isalnum():
            raise SparkEngineError("El esquema contiene un nombre de columna inválido.")
        if column_name in MYSQL_STRING_COLUMNS:
            mysql_type = MYSQL_STRING_COLUMNS[column_name]
        elif column_name == "date":
            mysql_type = "DATE"
        else:
            mysql_type = "DOUBLE"

        not_null = " NOT NULL" if column_name in {"location", "date"} else ""
        column_definitions.append(f"`{column_name}` {mysql_type}{not_null}")

    column_definitions.extend(
        (
            "PRIMARY KEY (`location`, `date`)",
            "KEY `idx_seguimiento_date` (`date`)",
            "KEY `idx_seguimiento_iso_code` (`iso_code`)",
        )
    )
    return (
        f"CREATE TABLE `{table_name}` ({', '.join(column_definitions)}) "
        "ENGINE=InnoDB"
    )


def recrear_tabla_aiven(db_config: DatabaseConfig) -> dict[str, Any]:
    """Drop and recreate the empty epidemiological table in Aiven MySQL."""
    if db_config.target != "cloud":
        raise SparkEngineError("El reseteo solo está permitido para Aiven Cloud.")

    engine = create_sqlalchemy_engine(db_config)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"DROP TABLE IF EXISTS `{TARGET_TABLE}`"
            )
            connection.exec_driver_sql(
                _mysql_empty_table_ddl(TARGET_TABLE)
            )
    except Exception as exc:
        logger.exception("No se pudo recrear la tabla epidemiológica en Aiven")
        raise SparkEngineError(
            "No fue posible recrear la tabla en Aiven. Verifica la conexión y los permisos."
        ) from exc
    finally:
        engine.dispose()

    return {
        "table": TARGET_TABLE,
        "database": db_config.database,
        "target": db_config.target,
        "rows": 0,
    }


def cargar_a_mysql(db_config: DatabaseConfig) -> dict[str, Any]:
    """Atomically replace the MySQL table with a PK-constrained Spark load."""
    with _lock:
        if _processed_df is None:
            raise SparkEngineError("Primero ejecuta la transformación ETL.")

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
        engine = create_sqlalchemy_engine(db_config)
        staging_created = False
        swap_completed = False

        try:
            create_table_sql = _mysql_staging_table_ddl(
                _processed_df,
                staging_table,
            )
            with engine.connect() as connection:
                connection.exec_driver_sql(create_table_sql)
                staging_created = True

                (
                    _processed_df.write.format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", staging_table)
                    .option("user", db_config.username)
                    .option("password", db_config.password)
                    .option("driver", JDBC_DRIVER)
                    .option("truncate", "true")
                    .mode("overwrite")
                    .save()
                )

                target_exists = connection.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = :database AND table_name = :table"
                    ),
                    {"database": db_config.database, "table": TARGET_TABLE},
                ).scalar_one() > 0

                if target_exists:
                    connection.exec_driver_sql(
                        f"RENAME TABLE {quoted_target} TO {quoted_backup}, "
                        f"{quoted_staging} TO {quoted_target}"
                    )
                    swap_completed = True
                    try:
                        connection.exec_driver_sql(f"DROP TABLE {quoted_backup}")
                    except Exception:
                        logger.warning(
                            "La carga se completó, pero no se pudo eliminar la tabla de respaldo %s",
                            backup_table,
                            exc_info=True,
                        )
                else:
                    connection.exec_driver_sql(
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
            if staging_created and not swap_completed:
                try:
                    with engine.begin() as cleanup_connection:
                        cleanup_connection.exec_driver_sql(
                            f"DROP TABLE IF EXISTS {quoted_staging}"
                        )
                except Exception:
                    logger.warning(
                        "No se pudo limpiar la tabla temporal %s",
                        staging_table,
                        exc_info=True,
                    )
            engine.dispose()

        return {
            "table": TARGET_TABLE,
            "rows": _processed_df.count(),
            "database": db_config.database,
            "target": db_config.target,
        }


def close_spark_session() -> None:
    """Release cached DataFrames, downloaded data, and the Spark session."""
    with _lock:
        if _spark is not None and not _spark_gateway_is_alive():
            _discard_spark_state()
            return
        if _raw_df is not None:
            _raw_df.unpersist()
        if _processed_df is not None:
            _processed_df.unpersist()
        if _spark is not None:
            _spark.stop()
        _discard_spark_state()
