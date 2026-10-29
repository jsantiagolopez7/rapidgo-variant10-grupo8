"""Pruebas del generador de eventos (contrato y reglas de inventario)."""
import json
import os
import sys
from collections import defaultdict

import jsonschema

RAIZ = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(RAIZ, "producer"))
from generador import GeneradorInventario  # noqa: E402

with open(os.path.join(RAIZ, "samples", "evento_inventario.schema.json"), encoding="utf-8") as f:
    SCHEMA = json.load(f)


def eventos(n_pedidos=300, **kw):
    gen = GeneradorInventario(seed=42, **kw)
    return [ev for _ in range(n_pedidos) for ev in gen.pedido()]


def test_todos_los_eventos_cumplen_el_contrato():
    for ev in eventos():
        jsonschema.validate(ev, SCHEMA)


def test_stock_nunca_es_negativo_y_es_consistente():
    for ev in eventos():
        assert ev["stock_after"] >= 0
        if ev["event_type"] == "STOCK_REPLENISHED":
            assert ev["stock_after"] == ev["stock_before"] + ev["qty_requested"]
        else:
            assert ev["stock_after"] == ev["stock_before"] - ev["qty_picked"]
            assert ev["qty_picked"] <= ev["qty_requested"]


def test_genera_stockouts_y_picking_lento():
    evs = eventos()
    assert any(ev["event_type"] == "ITEM_OUT_OF_STOCK" for ev in evs)
    lentos = [ev for ev in evs if ev["event_type"] != "STOCK_REPLENISHED"
              and ev["pick_start_ts"] < ev["pick_end_ts"]
              and _segundos(ev) > 8 * 60]
    assert lentos, "debe haber eventos de picking de más de 8 minutos para disparar la alerta"


def test_un_pedido_es_de_una_sola_tienda():
    tiendas_por_pedido = defaultdict(set)
    for ev in eventos():
        if ev["event_type"] != "STOCK_REPLENISHED":
            tiendas_por_pedido[ev["order_id"]].add(ev["store_id"])
    assert all(len(t) == 1 for t in tiendas_por_pedido.values())


def test_misma_semilla_mismos_eventos():
    assert eventos(20) == eventos(20)


def _segundos(ev):
    from datetime import datetime
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return (datetime.strptime(ev["pick_end_ts"], fmt) - datetime.strptime(ev["pick_start_ts"], fmt)).seconds
