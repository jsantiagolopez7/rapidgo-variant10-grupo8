"""DAG de orquestación batch del grupo 8 (Variante 10: Inventario y fulfillment).

Flujo (una vez al día, sobre el Data Lake que escribe Spark Structured Streaming):

    check_sources -> validate_raw -> aggregate_daily_kpis -> validate_curated -> generate_report

- check_sources:        raw y processed existen y tienen registros (XCom: conteos).
- validate_raw:         % de eventos de raw que no cumplen el contrato <= 5 %.
- aggregate_daily_kpis: KPIs por día y tienda -> datalake/curated/kpis_diarios/ (XCom: ruta y filas).
- validate_curated:     reglas de calidad sobre los KPIs (rangos, nulos, duplicados).
- generate_report:      reporte Markdown en airflow/reports/ (XCom: ruta del reporte).

Fallo controlado (docs/checkpoints/semana3.md): disparar el DAG con
    {"raw_subdir": "raw/vacio"}
hace fallar check_sources (raw vacío); tras 2 reintentos queda en failed y las
tasks siguientes quedan en upstream_failed. Ver ADR-003.
"""
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

import calidad_inventario as cal

LAKE = os.getenv("DATALAKE_PATH", "/opt/airflow/datalake")
REPORTS = os.getenv("REPORTS_PATH", "/opt/airflow/reports")

default_args = {
    "owner": "grupo8",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="rapidgo_g8_inventario_diario",
    description="Valida el Data Lake de inventario, calcula KPIs diarios por tienda y genera el reporte",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "raw_subdir": Param("raw/inventory_events", type="string",
                            description="Carpeta de raw dentro del Data Lake"),
        "processed_subdir": Param("processed/inventory_events", type="string",
                                  description="Carpeta de processed dentro del Data Lake"),
    },
    tags=["rapidgo", "grupo8", "inventario", "semana3"],
    doc_md=__doc__,
)
def rapidgo_g8_inventario_diario():

    @task
    def check_sources():
        p = get_current_context()["params"]
        raw = cal.contar_fuente(os.path.join(LAKE, p["raw_subdir"]))
        processed = cal.contar_fuente(os.path.join(LAKE, p["processed_subdir"]))
        print(f"raw: {raw['registros']} registros en {raw['archivos']} archivos")
        print(f"processed: {processed['registros']} registros en {processed['archivos']} archivos")
        return {"raw_path": raw["ruta"], "processed_path": processed["ruta"],
                "n_raw": raw["registros"], "n_processed": processed["registros"]}

    @task
    def validate_raw(fuentes: dict):
        df = pd.read_parquet(fuentes["raw_path"], columns=["raw_json"])
        resumen = cal.validar_raw(df)
        print(f"validate_raw: {resumen}")
        return resumen

    @task
    def aggregate_daily_kpis(fuentes: dict, resumen_raw: dict):
        cols = ["event_id", "event_type", "order_id", "pick_seconds",
                "is_stockout", "is_incomplete", "fecha", "store_id"]
        df = pd.read_parquet(fuentes["processed_path"], columns=cols)
        kpis = cal.kpis_diarios(df)
        ds = get_current_context()["ds"]
        destino = os.path.join(LAKE, "curated", "kpis_diarios")
        os.makedirs(destino, exist_ok=True)
        ruta = os.path.join(destino, f"kpis_diarios_{ds}.parquet")
        kpis.to_parquet(ruta, index=False)  # idempotente: sobrescribe el archivo del día
        print(f"aggregate_daily_kpis: {len(kpis)} filas -> {ruta} "
              f"(raw válidos: {resumen_raw['validos']})")
        return {"kpis_path": ruta, "filas": len(kpis)}

    @task
    def validate_curated(kpis_info: dict):
        resumen = cal.validar_curated(pd.read_parquet(kpis_info["kpis_path"]))
        print(f"validate_curated: {resumen}")
        return resumen

    @task
    def generate_report(kpis_info: dict, resumen_raw: dict, resumen_curated: dict):
        ds = get_current_context()["ds"]
        texto = cal.generar_reporte(pd.read_parquet(kpis_info["kpis_path"]), resumen_raw, ds)
        os.makedirs(REPORTS, exist_ok=True)
        ruta = os.path.join(REPORTS, f"reporte_inventario_{ds}.md")
        with open(ruta, "w", encoding="utf-8") as f:
            f.write(texto)
        print(texto)
        print(f"Reporte guardado en {ruta} ({resumen_curated['tiendas']} tiendas)")
        return ruta

    fuentes = check_sources()
    resumen_raw = validate_raw(fuentes)
    kpis_info = aggregate_daily_kpis(fuentes, resumen_raw)
    resumen_curated = validate_curated(kpis_info)
    generate_report(kpis_info, resumen_raw, resumen_curated)


rapidgo_g8_inventario_diario()
