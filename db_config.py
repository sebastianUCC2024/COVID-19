"""Environment-driven MySQL configuration for local and Aiven deployments."""

from __future__ import annotations

import os
from dataclasses import dataclass

from sqlalchemy import URL, Engine, create_engine


class DatabaseConfigurationError(ValueError):
    """Raised when the selected database target is not configured correctly."""


@dataclass(frozen=True)
class DatabaseConfig:
    target: str
    host: str
    port: int
    username: str
    password: str
    database: str
    jdbc_ssl_mode: str


def get_database_config(target: str) -> DatabaseConfig:
    """Build a database configuration from LOCAL_DB_* or AIVEN_DB_* variables."""
    if target not in {"local", "cloud"}:
        raise DatabaseConfigurationError("El destino debe ser 'local' o 'cloud'.")

    prefix = "LOCAL_DB" if target == "local" else "AIVEN_DB"
    values = {
        key: os.getenv(f"{prefix}_{key}")
        for key in ("HOST", "PORT", "USER", "PASSWORD", "NAME")
    }
    missing = [key for key, value in values.items() if value is None]
    if missing:
        names = ", ".join(f"{prefix}_{key}" for key in missing)
        raise DatabaseConfigurationError(f"Faltan variables de configuración: {names}.")

    empty_required = [
        key
        for key in ("HOST", "USER", "NAME")
        if not values[key].strip()
    ]
    if target == "cloud" and not values["PASSWORD"].strip():
        empty_required.append("PASSWORD")
    if empty_required:
        names = ", ".join(f"{prefix}_{key}" for key in empty_required)
        raise DatabaseConfigurationError(f"Las variables de configuración no pueden estar vacías: {names}.")

    try:
        port = int(values["PORT"])
    except ValueError as exc:
        raise DatabaseConfigurationError(f"{prefix}_PORT debe ser un número entero.") from exc
    if not 1 <= port <= 65535:
        raise DatabaseConfigurationError(f"{prefix}_PORT debe estar entre 1 y 65535.")

    return DatabaseConfig(
        target=target,
        host=values["HOST"].strip(),
        port=port,
        username=values["USER"],
        password=values["PASSWORD"],
        database=values["NAME"].strip(),
        jdbc_ssl_mode="DISABLED" if target == "local" else "REQUIRED",
    )


def create_sqlalchemy_engine(db_config: DatabaseConfig) -> Engine:
    """Create a pooled SQLAlchemy engine, enabling TLS for the cloud target."""
    url = URL.create(
        drivername="mysql+pymysql",
        username=db_config.username,
        password=db_config.password,
        host=db_config.host,
        port=db_config.port,
        database=db_config.database,
    )
    connect_args = {"ssl": {}} if db_config.target == "cloud" else {}
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args=connect_args,
    )
