"""Reglas de calidad, KPIs diarios y reporte del DAG de Airflow (Variante 10, grupo 8).

Son funciones puras sobre pandas, sin dependencias de Airflow, para poder
probarlas con pytest (tests/test_calidad_inventario.py) y reutilizarlas
desde las tasks del DAG (rapidgo_g8_inventario_dag.py).
"""
import glob
import json
import os

import pandas as pd
import pyarrow.parquet as pq

# Campos obligatorios del contrato (samples/evento_inventario.schema.json)
CAMPOS_OBLIGATORIOS = [
    "event_id", "team", "event_type", "event_ts", "store_id", "order_id", "sku",
    "qty_requested", "qty_picked", "stock_after", "pick_start_ts", "pick_end_ts",
]
EVENTOS_PICKING = ["ITEM_PICKED", "ITEM_OUT_OF_STOCK"]

# Umbrales de calidad
MAX_PCT_INVALIDOS_RAW = 0.05   # si más del 5 % del raw no cumple el contrato, se detiene el DAG
MIN_REGISTROS_RAW = 1          # raw vacío = fuente caída


class ErrorCalidad(Exception):
    """Falla de una regla de calidad: la task debe fallar y Airflow la reintenta."""


def archivos_parquet(ruta):
    """Lista los .parquet de una carpeta (incluye subcarpetas de partición)."""
    return sorted(glob.glob(os.path.join(ruta, "**", "*.parquet"), recursive=True))


def contar_fuente(ruta, minimo=MIN_REGISTROS_RAW):
    """check_sources: la carpeta existe y tiene al menos `minimo` registros."""
    if not os.path.isdir(ruta):
        raise ErrorCalidad(f"La fuente {ruta} no existe. ¿Corrió el job de Spark?")
    archivos = archivos_parquet(ruta)
    if not archivos:
        raise ErrorCalidad(f"La fuente {ruta} no tiene archivos Parquet (raw vacío).")
    n = sum(pq.ParquetFile(a).metadata.num_rows for a in archivos)
    if n < minimo:
        raise ErrorCalidad(f"La fuente {ruta} tiene {n} registros; se esperaban al menos {minimo}.")
    return {"ruta": ruta, "archivos": len(archivos), "registros": n}


def evento_cumple_contrato(raw_json):
    """True si el JSON se puede leer, trae todos los campos obligatorios y respeta las reglas básicas."""
    try:
        e = json.loads(raw_json)
    except (TypeError, ValueError):
        return False
    if not isinstance(e, dict) or any(e.get(c) is None for c in CAMPOS_OBLIGATORIOS):
        return False
    try:
        return (
            e["team"] == "grupo8"
            and int(e["qty_picked"]) <= int(e["qty_requested"])
            and int(e["stock_after"]) >= 0
            and str(e["pick_end_ts"]) >= str(e["pick_start_ts"])
        )
    except (TypeError, ValueError):
        return False


def validar_raw(df_raw, max_pct_invalidos=MAX_PCT_INVALIDOS_RAW):
    """validate_raw: porcentaje de eventos que no cumplen el contrato."""
    total = len(df_raw)
    if total == 0:
        raise ErrorCalidad("validate_raw: raw vacío, no hay nada que validar.")
    validos = int(df_raw["raw_json"].map(evento_cumple_contrato).sum())
    invalidos = total - validos
    pct = invalidos / total
    resultado = {"total": total, "validos": validos, "invalidos": invalidos,
                 "pct_invalidos": round(pct, 4)}
    if pct > max_pct_invalidos:
        raise ErrorCalidad(
            f"validate_raw: {invalidos} de {total} eventos ({pct:.1%}) no cumplen el contrato; "
            f"el máximo permitido es {max_pct_invalidos:.0%}.")
    return resultado


def kpis_diarios(df_processed):
    """aggregate_daily_kpis: KPIs por día y tienda a partir de la zona processed."""
    df = df_processed[df_processed["event_type"].isin(EVENTOS_PICKING)].copy()
    if df.empty:
        raise ErrorCalidad("aggregate_daily_kpis: processed no tiene eventos de picking.")
    df["fecha"] = df["fecha"].astype(str)
    df["store_id"] = df["store_id"].astype(str)
    pedidos = df.groupby(["fecha", "store_id", "order_id"])["is_incomplete"].max().reset_index()
    ped = pedidos.groupby(["fecha", "store_id"]).agg(
        n_pedidos=("order_id", "count"), n_pedidos_incompletos=("is_incomplete", "sum"))
    ev = df.groupby(["fecha", "store_id"]).agg(
        n_eventos=("event_id", "count"),
        n_stockouts=("is_stockout", "sum"),
        avg_pick_seconds=("pick_seconds", "mean"),
        p95_pick_seconds=("pick_seconds", lambda s: s.quantile(0.95)),
    )
    out = ev.join(ped).reset_index()
    out["stockout_rate"] = out["n_stockouts"] / out["n_eventos"]
    out["pct_pedidos_incompletos"] = out["n_pedidos_incompletos"] / out["n_pedidos"]
    for c in ["avg_pick_seconds", "p95_pick_seconds", "stockout_rate", "pct_pedidos_incompletos"]:
        out[c] = out[c].astype(float).round(4)
    for c in ["n_eventos", "n_stockouts", "n_pedidos", "n_pedidos_incompletos"]:
        out[c] = out[c].astype(int)
    return out.sort_values(["fecha", "store_id"]).reset_index(drop=True)


def validar_curated(df_kpis):
    """validate_curated: reglas de calidad sobre los KPIs antes de publicar el reporte."""
    errores = []
    if df_kpis.empty:
        errores.append("no hay filas de KPIs")
    else:
        if df_kpis[["fecha", "store_id"]].isnull().any().any():
            errores.append("hay fecha o store_id nulos")
        if df_kpis.duplicated(["fecha", "store_id"]).any():
            errores.append("hay filas duplicadas por (fecha, store_id)")
        for c in ["stockout_rate", "pct_pedidos_incompletos"]:
            if not df_kpis[c].between(0, 1).all():
                errores.append(f"{c} fuera del rango [0, 1]")
        if (df_kpis["avg_pick_seconds"] < 0).any():
            errores.append("avg_pick_seconds negativo")
        if (df_kpis["n_pedidos"] <= 0).any():
            errores.append("tiendas con 0 pedidos")
    if errores:
        raise ErrorCalidad("validate_curated: " + "; ".join(errores))
    return {"filas": len(df_kpis), "tiendas": int(df_kpis["store_id"].nunique()),
            "fechas": sorted(df_kpis["fecha"].unique().tolist())}


def generar_reporte(df_kpis, resumen_raw, fecha_ejecucion):
    """generate_report: reporte en Markdown con los KPIs por tienda."""
    tot_ev = int(df_kpis["n_eventos"].sum())
    tot_ped = int(df_kpis["n_pedidos"].sum())
    lineas = [
        f"# Reporte diario de inventario - grupo 8 ({fecha_ejecucion})",
        "",
        f"- Eventos en raw: {resumen_raw['total']} "
        f"({resumen_raw['invalidos']} no cumplen el contrato, {resumen_raw['pct_invalidos']:.2%})",
        f"- Eventos de picking procesados: {tot_ev}",
        f"- Tasa de stockout global: {df_kpis['n_stockouts'].sum() / tot_ev:.2%}",
        f"- Picking promedio global: "
        f"{(df_kpis['avg_pick_seconds'] * df_kpis['n_eventos']).sum() / tot_ev:.1f} s",
        f"- Pedidos incompletos: {df_kpis['n_pedidos_incompletos'].sum()} de {tot_ped}",
        "",
        "| Fecha | Tienda | Eventos | Stockout rate | Picking prom. (s) | Picking p95 (s) | % pedidos incompletos |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in df_kpis.itertuples():
        lineas.append(
            f"| {r.fecha} | {r.store_id} | {r.n_eventos} | {r.stockout_rate:.2%} | "
            f"{r.avg_pick_seconds:.1f} | {r.p95_pick_seconds:.1f} | {r.pct_pedidos_incompletos:.2%} |")
    return "\n".join(lineas) + "\n"
