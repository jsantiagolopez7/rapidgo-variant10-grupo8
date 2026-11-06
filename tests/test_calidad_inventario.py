"""Pruebas de las reglas de calidad y KPIs diarios que usa el DAG de Airflow."""
import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
import calidad_inventario as cal  # noqa: E402


def evento(**kw):
    base = {
        "event_id": "e1", "team": "grupo8", "event_type": "ITEM_PICKED",
        "event_ts": "2026-11-03T14:03:11Z", "store_id": "STORE-03",
        "order_id": "ORD-000123", "sku": "SKU-1042", "category": "lacteos",
        "qty_requested": 3, "qty_picked": 3, "stock_before": 20, "stock_after": 17,
        "picker_id": "PK-07", "pick_start_ts": "2026-11-03T14:01:40Z",
        "pick_end_ts": "2026-11-03T14:03:11Z",
    }
    base.update(kw)
    return base


def raw_df(eventos):
    return pd.DataFrame({"raw_json": [e if isinstance(e, str) else json.dumps(e) for e in eventos]})


def processed_df():
    filas = [
        # STORE-01: 2 pedidos, uno incompleto por un stockout
        ("a1", "ITEM_PICKED", "O1", 100, 0, 0, "2026-11-03", "STORE-01"),
        ("a2", "ITEM_OUT_OF_STOCK", "O1", 40, 1, 1, "2026-11-03", "STORE-01"),
        ("a3", "ITEM_PICKED", "O2", 200, 0, 0, "2026-11-03", "STORE-01"),
        # STORE-02: 1 pedido completo
        ("b1", "ITEM_PICKED", "O3", 300, 0, 0, "2026-11-03", "STORE-02"),
        # las reposiciones no cuentan para los KPIs de picking
        ("r1", "STOCK_REPLENISHED", "-", 0, 0, 0, "2026-11-03", "STORE-02"),
    ]
    cols = ["event_id", "event_type", "order_id", "pick_seconds", "is_stockout",
            "is_incomplete", "fecha", "store_id"]
    return pd.DataFrame(filas, columns=cols)


# ---------- contrato en raw ----------

def test_evento_valido_cumple_contrato():
    assert cal.evento_cumple_contrato(json.dumps(evento()))


@pytest.mark.parametrize("raw", [
    "{no es json",
    json.dumps({k: v for k, v in evento().items() if k != "store_id"}),  # falta campo
    json.dumps(evento(team="grupo3")),                                   # otro equipo
    json.dumps(evento(qty_picked=5, qty_requested=3)),                   # picked > requested
    json.dumps(evento(stock_after=-1)),                                  # stock negativo
    json.dumps(evento(pick_end_ts="2026-11-03T14:00:00Z")),              # fin antes del inicio
])
def test_evento_invalido_no_cumple_contrato(raw):
    assert not cal.evento_cumple_contrato(raw)


def test_validar_raw_pasa_bajo_el_umbral():
    eventos = [evento(event_id=f"e{i}") for i in range(99)] + ["{roto"]
    r = cal.validar_raw(raw_df(eventos))
    assert r == {"total": 100, "validos": 99, "invalidos": 1, "pct_invalidos": 0.01}


def test_validar_raw_falla_si_supera_el_umbral():
    eventos = [evento(event_id=f"e{i}") for i in range(9)] + ["{roto"]  # 10 % inválido
    with pytest.raises(cal.ErrorCalidad, match="no cumplen el contrato"):
        cal.validar_raw(raw_df(eventos))


def test_validar_raw_falla_si_esta_vacio():
    with pytest.raises(cal.ErrorCalidad, match="raw vacío"):
        cal.validar_raw(raw_df([]))


# ---------- check_sources ----------

def test_contar_fuente_inexistente(tmp_path):
    with pytest.raises(cal.ErrorCalidad, match="no existe"):
        cal.contar_fuente(str(tmp_path / "raw" / "inventory_events"))


def test_contar_fuente_vacia(tmp_path):
    (tmp_path / "raw").mkdir()
    with pytest.raises(cal.ErrorCalidad, match="raw vacío"):
        cal.contar_fuente(str(tmp_path / "raw"))


def test_contar_fuente_con_particiones(tmp_path):
    processed_df().to_parquet(tmp_path / "processed", partition_cols=["fecha", "store_id"])
    r = cal.contar_fuente(str(tmp_path / "processed"))
    assert r["registros"] == 5
    assert r["archivos"] == 2


# ---------- KPIs diarios ----------

def test_kpis_diarios_por_tienda():
    k = cal.kpis_diarios(processed_df()).set_index("store_id")
    s1 = k.loc["STORE-01"]
    assert s1["n_eventos"] == 3
    assert s1["n_stockouts"] == 1
    assert s1["stockout_rate"] == pytest.approx(1 / 3, abs=1e-4)
    assert s1["avg_pick_seconds"] == pytest.approx(340 / 3, abs=1e-3)
    assert s1["n_pedidos"] == 2
    assert s1["n_pedidos_incompletos"] == 1
    assert s1["pct_pedidos_incompletos"] == 0.5
    s2 = k.loc["STORE-02"]
    assert s2["n_eventos"] == 1          # la reposición no cuenta
    assert s2["stockout_rate"] == 0


def test_kpis_diarios_sin_picking_falla():
    df = processed_df()
    with pytest.raises(cal.ErrorCalidad):
        cal.kpis_diarios(df[df["event_type"] == "STOCK_REPLENISHED"])


# ---------- validate_curated ----------

def test_validar_curated_ok():
    r = cal.validar_curated(cal.kpis_diarios(processed_df()))
    assert r == {"filas": 2, "tiendas": 2, "fechas": ["2026-11-03"]}


@pytest.mark.parametrize("cambio, mensaje", [
    (lambda k: k.assign(stockout_rate=1.5), "stockout_rate fuera"),
    (lambda k: k.assign(avg_pick_seconds=-1.0), "negativo"),
    (lambda k: k.assign(n_pedidos=0), "0 pedidos"),
    (lambda k: pd.concat([k, k]), "duplicadas"),
    (lambda k: k.iloc[0:0], "no hay filas"),
])
def test_validar_curated_detecta_errores(cambio, mensaje):
    with pytest.raises(cal.ErrorCalidad, match=mensaje):
        cal.validar_curated(cambio(cal.kpis_diarios(processed_df())))


# ---------- reporte ----------

def test_generar_reporte():
    kpis = cal.kpis_diarios(processed_df())
    resumen = {"total": 6, "validos": 5, "invalidos": 1, "pct_invalidos": 1 / 6}
    texto = cal.generar_reporte(kpis, resumen, "2026-11-03")
    assert texto.startswith("# Reporte diario de inventario - grupo 8 (2026-11-03)")
    assert "| 2026-11-03 | STORE-01 | 3 | 33.33% |" in texto
    assert "Pedidos incompletos: 1 de 3" in texto
