"""Resumen del Data Lake para los checkpoints y el ADR-002.

Ejecutar (con el pipeline ya corriendo un rato):
  docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/spark/revisar_datalake.py
"""
import os

from pyspark.sql import SparkSession, functions as F

LAKE = os.getenv("DATALAKE_PATH", "/opt/app/datalake")

spark = (SparkSession.builder.appName("g8-revisar-datalake")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def leer(ruta):
    try:
        return spark.read.parquet(f"{LAKE}/{ruta}")
    except Exception:  # la carpeta aún no existe o está vacía
        print(f"  (sin datos todavía en {ruta})")
        return None


print("\n=== Registros por zona ===")
for ruta in ["raw/inventory_events", "processed/inventory_events",
             "curated/kpis_tienda_5min", "curated/alertas"]:
    df = leer(ruta)
    if df is not None:
        print(f"{ruta}: {df.count()}")

raw = leer("raw/inventory_events")
if raw is not None:
    print("\n=== Mensajes por partición de Kafka (key = store_id) ===")
    raw.groupBy("partition").count().orderBy("partition").show()

processed = leer("processed/inventory_events")
if processed is not None:
    print("=== Eventos por tienda ===")
    processed.groupBy("store_id").count().orderBy("store_id").show()
    print("=== Indicadores globales ===")
    processed.filter(F.col("event_type") != "STOCK_REPLENISHED").agg(
        F.round(F.avg("pick_seconds"), 1).alias("picking_promedio_seg"),
        F.expr("percentile_approx(pick_seconds, 0.95)").alias("picking_p95_seg"),
        F.round(F.avg("is_stockout") * 100, 2).alias("pct_stockout"),
    ).show()

alertas = leer("curated/alertas")
if alertas is not None:
    print("=== Alertas por tipo y severidad ===")
    alertas.groupBy("alert_type", "severity").count().orderBy("alert_type").show()

kpis = leer("curated/kpis_tienda_5min")
if kpis is not None:
    print("=== Últimas ventanas de KPIs ===")
    kpis.orderBy(F.desc("window_start")).show(10, truncate=False)
