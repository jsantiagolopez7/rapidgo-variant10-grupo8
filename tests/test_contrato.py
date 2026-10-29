"""Pruebas del contrato del evento (samples/evento_inventario.schema.json)."""
import copy
import json
import os

import jsonschema
import pytest

RAIZ = os.path.join(os.path.dirname(__file__), "..")


def _leer(nombre):
    with open(os.path.join(RAIZ, "samples", nombre), encoding="utf-8") as f:
        return json.load(f)


SCHEMA = _leer("evento_inventario.schema.json")
EJEMPLO = _leer("evento_inventario.json")


def test_el_ejemplo_cumple_el_contrato():
    jsonschema.validate(EJEMPLO, SCHEMA)


@pytest.mark.parametrize("campo", ["event_id", "store_id", "sku", "qty_requested", "stock_after"])
def test_falta_un_campo_obligatorio(campo):
    ev = copy.deepcopy(EJEMPLO)
    del ev[campo]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(ev, SCHEMA)


@pytest.mark.parametrize("campo,valor", [
    ("team", "grupo5"),               # eventos de otro equipo
    ("store_id", "TIENDA-1"),         # formato de tienda inválido
    ("qty_requested", 0),             # un pedido pide al menos 1 unidad
    ("stock_after", -1),              # el stock no puede ser negativo
    ("event_type", "ORDER_CANCELLED"),
    ("campo_extra", "x"),             # no se aceptan campos fuera del contrato
])
def test_valores_invalidos(campo, valor):
    ev = copy.deepcopy(EJEMPLO)
    ev[campo] = valor
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(ev, SCHEMA)


def test_agotado_debe_dejar_stock_en_cero():
    ev = copy.deepcopy(EJEMPLO)
    ev.update({"event_type": "ITEM_OUT_OF_STOCK", "qty_picked": 0, "stock_after": 3})
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(ev, SCHEMA)
