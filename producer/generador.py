"""Generador sintético de eventos de inventario y picking (Variante 10, Grupo 8).

Simula tiendas, SKUs y pickers. Cada pedido tiene de 1 a 4 líneas y cada línea
produce un evento que cumple samples/evento_inventario.schema.json.
No depende de Kafka, así que se puede probar con pytest.
"""
import random
import uuid
from datetime import datetime, timedelta, timezone

CATEGORIAS = ["lacteos", "aseo", "carnes", "bebidas", "granos"]
ZONAS = ["pasillo-1", "pasillo-2", "pasillo-3", "pasillo-4", "bodega"]


def _iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


class GeneradorInventario:
    def __init__(self, n_tiendas=8, n_skus=40, n_pickers=15, prob_picking_lento=0.05,
                 prob_reposicion=0.03, stock_inicial_max=30, seed=None):
        self.rng = random.Random(seed)
        self.tiendas = [f"STORE-{i:02d}" for i in range(1, n_tiendas + 1)]
        self.skus = {f"SKU-{1000 + i:04d}": self.rng.choice(CATEGORIAS) for i in range(1, n_skus + 1)}
        self.pickers = [f"PK-{i:02d}" for i in range(1, n_pickers + 1)]
        self.prob_picking_lento = prob_picking_lento
        self.prob_reposicion = prob_reposicion
        self.stock = {(t, s): self.rng.randint(0, stock_inicial_max)
                      for t in self.tiendas for s in self.skus}
        self.siguiente_pedido = 1

    def _evento_base(self, tienda, sku, ahora):
        return {
            "schema_version": "1.0",
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "team": "grupo8",
            "event_ts": _iso(ahora),
            "store_id": tienda,
            "sku": sku,
            "category": self.skus[sku],
            "zone": self.rng.choice(ZONAS),
        }

    def _duracion_picking(self):
        if self.rng.random() < self.prob_picking_lento:
            return self.rng.randint(9 * 60, 15 * 60)  # picking lento (> 8 min)
        return self.rng.randint(30, 240)

    def reposicion(self, tienda, sku, ahora):
        """Evento STOCK_REPLENISHED: llega mercancía a la tienda."""
        cantidad = self.rng.randint(10, 30)
        antes = self.stock[(tienda, sku)]
        self.stock[(tienda, sku)] = antes + cantidad
        ev = self._evento_base(tienda, sku, ahora)
        ev.update({
            "event_type": "STOCK_REPLENISHED",
            "order_id": "ORD-000000",
            "priority": "normal",
            "qty_requested": cantidad,
            "qty_picked": 0,
            "stock_before": antes,
            "stock_after": antes + cantidad,
            "picker_id": self.rng.choice(self.pickers),
            "pick_start_ts": _iso(ahora),
            "pick_end_ts": _iso(ahora),
        })
        return ev

    def pedido(self, ahora=None):
        """Devuelve la lista de eventos de un pedido (una por línea)."""
        ahora = ahora or datetime.now(timezone.utc)
        tienda = self.rng.choice(self.tiendas)
        order_id = f"ORD-{self.siguiente_pedido:06d}"
        self.siguiente_pedido = self.siguiente_pedido % 999999 + 1
        picker = self.rng.choice(self.pickers)
        prioridad = self.rng.choice(["normal", "normal", "express"])
        eventos = []
        for sku in self.rng.sample(sorted(self.skus), self.rng.randint(1, 4)):
            if self.rng.random() < self.prob_reposicion:
                eventos.append(self.reposicion(tienda, sku, ahora))
            pedido_qty = self.rng.randint(1, 5)
            antes = self.stock[(tienda, sku)]
            duracion = self._duracion_picking()
            ev = self._evento_base(tienda, sku, ahora)
            if antes == 0:
                tipo, recogido = "ITEM_OUT_OF_STOCK", 0
            else:
                tipo, recogido = "ITEM_PICKED", min(pedido_qty, antes)
            self.stock[(tienda, sku)] = antes - recogido
            ev.update({
                "event_type": tipo,
                "order_id": order_id,
                "priority": prioridad,
                "qty_requested": pedido_qty,
                "qty_picked": recogido,
                "stock_before": antes,
                "stock_after": antes - recogido,
                "picker_id": picker,
                "pick_start_ts": _iso(ahora - timedelta(seconds=duracion)),
                "pick_end_ts": _iso(ahora),
            })
            eventos.append(ev)
        return eventos
