import json
import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
from transformations import (  # noqa: E402
    parse_events, validate_events, enrich_events, kpis_por_tienda,
    alerta_stock_critico, alerta_picking_lento,
)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests-g8")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()


def evento(**kw):
    base = {
        "event_id": "e1", "team": "grupo8", "event_type": "ITEM_PICKED",
        "event_ts": "2026-10-22T14:03:11Z", "store_id": "STORE-03",
        "order_id": "ORD-000123", "sku": "SKU-1042", "category": "lacteos",
        "qty_requested": 3, "qty_picked": 3, "stock_before": 20, "stock_after": 17,
        "picker_id": "PK-07", "pick_start_ts": "2026-10-22T14:01:40Z",
        "pick_end_ts": "2026-10-22T14:03:11Z",
    }
    base.update(kw)
    return base


def kafka_df(spark, eventos):
    """Simula lo que entrega Kafka: columna value (bytes) + metadatos."""
    rows = [(json.dumps(e).encode(), "t", 0, i, datetime(2026, 10, 22, 14, 3, 11))
            for i, e in enumerate(eventos)]
    return spark.createDataFrame(rows, "value binary, topic string, partition int, offset long, timestamp timestamp")


def procesar(spark, eventos):
    return enrich_events(validate_events(parse_events(kafka_df(spark, eventos))))


def test_parseo_y_columnas_derivadas(spark):
    fila = procesar(spark, [evento()]).collect()[0]
    assert fila["pick_seconds"] == 91
    assert fila["is_stockout"] == 0
    assert str(fila["fecha"]) == "2026-10-22"


def test_validacion_descarta_eventos_invalidos(spark):
    eventos = [
        evento(event_id="ok"),
        evento(event_id="mas_de_lo_pedido", qty_picked=9),
        evento(event_id="otro_equipo", team="grupo1"),
        evento(event_id="fin_antes_de_inicio", pick_end_ts="2026-10-22T14:00:00Z"),
    ]
    ids = [r["event_id"] for r in procesar(spark, eventos).collect()]
    assert ids == ["ok"]


def test_json_corrupto_se_descarta(spark):
    df = spark.createDataFrame(
        [(b"esto no es json", "t", 0, 0, datetime(2026, 10, 22))],
        "value binary, topic string, partition int, offset long, timestamp timestamp")
    assert validate_events(parse_events(df)).count() == 0


def test_alerta_stock_critico(spark):
    eventos = [
        evento(event_id="a", stock_after=0, event_type="ITEM_OUT_OF_STOCK", qty_picked=0),
        evento(event_id="b", stock_after=5),
        evento(event_id="c", stock_after=6),
    ]
    res = {r["event_id"]: r["severity"] for r in alerta_stock_critico(procesar(spark, eventos)).collect()}
    assert res == {"a": "ALTA", "b": "MEDIA"}


def test_alerta_picking_lento(spark):
    eventos = [
        evento(event_id="lento", pick_start_ts="2026-10-22T14:00:00Z", pick_end_ts="2026-10-22T14:09:30Z"),
        evento(event_id="justo", pick_start_ts="2026-10-22T14:00:00Z", pick_end_ts="2026-10-22T14:08:00Z"),
    ]
    ids = [r["event_id"] for r in alerta_picking_lento(procesar(spark, eventos)).collect()]
    assert ids == ["lento"]


def test_kpis_por_tienda(spark):
    eventos = [
        evento(event_id="1", order_id="O1"),
        evento(event_id="2", order_id="O2", event_type="ITEM_OUT_OF_STOCK", qty_picked=0, stock_after=0),
        evento(event_id="3", order_id="O3", qty_picked=1),
        evento(event_id="4", order_id="O4", store_id="STORE-05"),
    ]
    kpis = {r["store_id"]: r for r in kpis_por_tienda(procesar(spark, eventos)).collect()}
    s3 = kpis["STORE-03"]
    assert s3["n_eventos"] == 3
    assert s3["stockout_rate"] == pytest.approx(1 / 3)
    assert s3["avg_pick_seconds"] == pytest.approx(91)
    assert s3["pct_pedidos_incompletos"] == pytest.approx(2 / 3)
    assert kpis["STORE-05"]["stockout_rate"] == 0
