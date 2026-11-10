"""Script de verificación y diagnóstico rápido del entorno para el Proyecto 6."""

from __future__ import annotations

import os
import shutil
import sys
import time
from pathlib import Path

# Intentar importar dotenv
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"


def print_check(name: str, passed: bool, detail: str = "") -> None:
    status = f"{GREEN}[OK]{RESET}" if passed else f"{RED}[FALLÓ]{RESET}"
    print(f" {status} {name}: {detail}")


def main() -> None:
    print(f"\n{CYAN}====================================================={RESET}")
    print(f"{CYAN} DIAGNÓSTICO DEL ENTORNO - SEGUIMIENTO COVID-19/OWID {RESET}")
    print(f"{CYAN}====================================================={RESET}\n")

    # 1. Comprobar Python
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    py_ok = sys.version_info >= (3, 10)
    print_check("Versión de Python", py_ok, f"{py_ver} (Requerido: >= 3.10)")

    # 2. Comprobar Java (requerido por PySpark)
    java_bin = shutil.which("java")
    java_home = os.getenv("JAVA_HOME")
    java_ok = java_bin is not None or (java_home and Path(java_home).exists())
    detail_java = f"Detectado en {java_bin or java_home}" if java_ok else "No encontrado en PATH ni JAVA_HOME (OK si ejecutas en Docker)"
    print_check("Entorno Java (PySpark)", java_ok, detail_java)

    # 3. Comprobar dependencias clave
    for pkg in ("fastapi", "pyspark", "sqlalchemy", "pymysql", "requests"):
        try:
            __import__(pkg)
            print_check(f"Paquete Python '{pkg}'", True, "Instalado")
        except ImportError:
            print_check(f"Paquete Python '{pkg}'", False, "Falta instalar en este entorno")

    # 4. Comprobar variables de entorno
    print(f"\n{YELLOW}[Verificación de Variables de Base de Datos]{RESET}")
    target = "cloud" if os.getenv("AIVEN_DB_HOST") else "local"
    prefix = "AIVEN_DB" if target == "cloud" else "LOCAL_DB"
    host = os.getenv(f"{prefix}_HOST")
    port = os.getenv(f"{prefix}_PORT")
    user = os.getenv(f"{prefix}_USER")
    database = os.getenv(f"{prefix}_NAME")

    vars_ok = bool(host and port and user and database)
    print_check(
        f"Variables de destino ({target})",
        vars_ok,
        f"Host={host or 'FALTA'}, Port={port or 'FALTA'}, User={user or 'FALTA'}, DB={database or 'FALTA'}"
    )

    # 5. Probar conexión y latencia hacia la base de datos
    if vars_ok:
        print(f"\n{YELLOW}[Prueba de Conexión a Base de Datos]{RESET}")
        try:
            from db_config import get_database_config, create_sqlalchemy_engine
            from sqlalchemy import text

            cfg = get_database_config(target)
            engine = create_sqlalchemy_engine(cfg)

            start = time.perf_counter()
            with engine.connect() as conn:
                res = conn.execute(text("SELECT 1")).scalar_one()
                latency_ms = (time.perf_counter() - start) * 1000

            print_check(
                f"Conexión a MySQL ({cfg.host}:{cfg.port})",
                True,
                f"Latencia de ping: {latency_ms:.1f} ms | SSL: {cfg.jdbc_ssl_mode}"
            )

            # Verificar si la tabla existe
            with engine.connect() as conn:
                table_exists = conn.execute(
                    text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :db AND table_name = 'seguimiento_epidemiologico'"),
                    {"db": cfg.database}
                ).scalar_one() > 0

                if table_exists:
                    rows = conn.execute(text("SELECT COUNT(*) FROM `seguimiento_epidemiologico`")).scalar_one()
                    print_check("Tabla 'seguimiento_epidemiologico'", True, f"Existe con {rows:,} registros cargados")
                else:
                    print_check("Tabla 'seguimiento_epidemiologico'", True, "Lista para ser creada por el pipeline")

            engine.dispose()
        except Exception as exc:
            print_check("Conexión a MySQL", False, str(exc))

    print(f"\n{CYAN}====================================================={RESET}")
    print(f"{CYAN} DIAGNÓSTICO FINALIZADO                              {RESET}")
    print(f"{CYAN}====================================================={RESET}\n")


if __name__ == "__main__":
    main()

