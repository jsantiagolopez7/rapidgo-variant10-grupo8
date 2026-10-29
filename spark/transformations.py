"""Lógica de negocio del job de Spark (Variante 10: Inventario y fulfillment).

Todas las funciones reciben y devuelven DataFrames, así que se pueden probar
con datos de ejemplo (tests/) y usar en streaming (streaming_job.py).
"""
from pyspark.sql import DataFrame, functions as F, types as T

# Umbrales iniciales (ver docs/arquitectura.md; se ajustan en el ADR-002)
STOCK_CRITICO_MAX = 5           # alerta si stock_after <= 5
PICKING_LENTO_SEGUNDOS = 8 * 60  # alerta si el picking dura más de 8 minutos
VENTANA_KPIS = "5 minutes"

# Contrato del evento (samples/evento_inventario.schema.json)
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("team", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.TimestampType()),
    T.StructField("store_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("sku", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("qty_requested", T.IntegerType()),
    T.StructField("qty_picked", T.IntegerType()),
    T.StructField("stock_before", T.IntegerType()),
    T.StructField("stock_after", T.IntegerType()),
    T.StructField("picker_id", T.StringType()),
    T.StructField("pick_start_ts", T.TimestampType()),
    T.StructField("pick_end_ts", T.TimestampType()),
])

REQUIRED_FIELDS = [
    "event_id", "team", "event_type", "event_ts", "store_id", "order_id", "sku",
    "qty_requested", "qty_picked", "stock_after", "pick_start_ts", "pick_end_ts",
]

PICKING_EVENTS = ["ITEM_PICKED", "ITEM_OUT_OF_STOCK"]


def parse_events(kafka_df: DataFrame) -> DataFrame:
    """Transformación 1: convierte el valor binario de Kafka en columnas del contrato."""
    return (
        kafka_df
        .select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic"), F.col("partition"), F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("e", F.from_json("raw_json", EVENT_SCHEMA))
        .select("raw_json", "topic", "partition", "offset", "kafka_ts", "e.*")
    )


def validate_events(df: DataFrame) -> DataFrame:
    """Transformación 2: deja solo los eventos que cumplen el contrato y sus reglas."""
    cond = F.lit(True)
    for c in REQUIRED_FIELDS:
        cond = cond & F.col(c).isNotNull()
    cond = (
        cond
        & (F.col("team") == "grupo8")
        & (F.col("qty_picked") <= F.col("qty_requested"))
        & (F.col("pick_end_ts") >= F.col("pick_start_ts"))
    )
    return df.filter(cond)


def enrich_events(df: DataFrame) -> DataFrame:
    """Transformación 3: columnas derivadas que usan los KPIs y las alertas."""
    return (
        df
        .withColumn("pick_seconds", F.col("pick_end_ts").cast("long") - F.col("pick_start_ts").cast("long"))
        .withColumn("is_stockout", (F.col("event_type") == "ITEM_OUT_OF_STOCK").cast("int"))
        .withColumn("is_incomplete", (F.col("qty_picked") < F.col("qty_requested")).cast("int"))
        .withColumn("fecha", F.to_date("event_ts"))
    )


def kpis_por_tienda(df: DataFrame, ventana: str = VENTANA_KPIS) -> DataFrame:
    """Agregación por ventana: KPIs por tienda cada 5 minutos.

    - stockout_rate: eventos ITEM_OUT_OF_STOCK / eventos de picking
    - avg/p95_pick_seconds: duración del picking por evento
    - pct_pedidos_incompletos: pedidos con alguna línea sin surtir / pedidos de la ventana
    """
    picking = df.filter(F.col("event_type").isin(PICKING_EVENTS))
    agg = (
        picking
        .groupBy(F.window("event_ts", ventana).alias("w"), "store_id")
        .agg(
            F.count("*").alias("n_eventos"),
            F.sum("is_stockout").alias("n_stockouts"),
            F.avg("pick_seconds").alias("avg_pick_seconds"),
            F.expr("percentile_approx(pick_seconds, 0.95)").alias("p95_pick_seconds"),
            F.approx_count_distinct("order_id").alias("n_pedidos"),
            F.approx_count_distinct(F.when(F.col("is_incomplete") == 1, F.col("order_id")))
             .alias("n_pedidos_incompletos"),
        )
    )
    return agg.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "store_id", "n_eventos", "n_stockouts",
        (F.col("n_stockouts") / F.col("n_eventos")).alias("stockout_rate"),
        "avg_pick_seconds", "p95_pick_seconds", "n_pedidos", "n_pedidos_incompletos",
        (F.col("n_pedidos_incompletos") / F.col("n_pedidos")).alias("pct_pedidos_incompletos"),
    )


_ALERT_COLS = ["alert_type", "severity", "event_id", "event_ts", "store_id", "order_id",
               "sku", "stock_after", "pick_seconds", "detected_at", "fecha"]


def alerta_stock_critico(df: DataFrame) -> DataFrame:
    """Alerta 1: stock_after <= 5 (severidad ALTA si el stock queda en 0)."""
    return (
        df.filter(F.col("event_type").isin(PICKING_EVENTS) & (F.col("stock_after") <= STOCK_CRITICO_MAX))
        .withColumn("alert_type", F.lit("STOCK_CRITICO"))
        .withColumn("severity", F.when(F.col("stock_after") == 0, "ALTA").otherwise("MEDIA"))
        .withColumn("detected_at", F.current_timestamp())
        .select(*_ALERT_COLS)
    )


def alerta_picking_lento(df: DataFrame) -> DataFrame:
    """Alerta 2: picking de más de 8 minutos."""
    return (
        df.filter(F.col("event_type").isin(PICKING_EVENTS) & (F.col("pick_seconds") > PICKING_LENTO_SEGUNDOS))
        .withColumn("alert_type", F.lit("PICKING_LENTO"))
        .withColumn("severity", F.lit("MEDIA"))
        .withColumn("detected_at", F.current_timestamp())
        .select(*_ALERT_COLS)
    )


def construir_alertas(df: DataFrame) -> DataFrame:
    return alerta_stock_critico(df).unionByName(alerta_picking_lento(df))
