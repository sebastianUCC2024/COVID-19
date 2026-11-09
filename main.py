"""FastAPI application for the respiratory disease epidemiology dashboard."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from db_config import DatabaseConfigurationError, get_database_config
from spark_engine import (
    SparkEngineError,
    close_spark_session,
    extraer_dataset,
    transformar_datos_pyspark,
    cargar_a_mysql,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await run_in_threadpool(close_spark_session)


app = FastAPI(
    title="Seguimiento Epidemiológico",
    description=(
        "Pipeline ETL de datos epidemiológicos RS-DELVE con PySpark y MySQL. "
        "Fuente CSV: https://raw.githubusercontent.com/rs-delve/covid19_datasets/"
        "master/dataset/combined_dataset_latest.csv"
    ),
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


class LoadRequest(BaseModel):
    database: Literal["local", "cloud"]


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "data": None, "message": str(exc.detail)},
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(
    _: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "status": "error",
            "data": {
                "invalid_fields": sorted(
                    {
                        ".".join(str(part) for part in error["loc"])
                        for error in exc.errors()
                    }
                )
            },
            "message": "La solicitud contiene campos inválidos.",
        },
    )


@app.exception_handler(Exception)
async def unexpected_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.error(
        "Error inesperado en la API",
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "data": None,
            "message": "Ocurrió un error interno. Consulta los logs del servidor.",
        },
    )


@app.get("/", include_in_schema=False)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={},
    )


@app.get("/health")
async def health_check() -> dict[str, Any]:
    return {"status": "success", "data": {"service": "api"}, "message": "Disponible"}


@app.post(
    "/api/extract",
    description="Descarga el CSV consolidado RS-DELVE y lo lee con PySpark.",
)
async def extract_data() -> dict:
    try:
        result = await run_in_threadpool(extraer_dataset)
    except SparkEngineError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"status": "success", "data": result, "message": "Dataset descargado"}


@app.post("/api/transform")
async def transform_data() -> dict:
    try:
        result = await run_in_threadpool(transformar_datos_pyspark)
    except SparkEngineError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "success", "data": result, "message": "ETL completado"}


@app.post("/api/load")
async def load_data(payload: LoadRequest) -> dict:
    try:
        database = get_database_config(payload.database)
    except DatabaseConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        result = await run_in_threadpool(cargar_a_mysql, database)
    except SparkEngineError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"status": "success", "data": result, "message": "Datos cargados en MySQL"}
