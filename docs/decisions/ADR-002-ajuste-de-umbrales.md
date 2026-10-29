# ADR-002 - La alerta de stock crítico solo se dispara cuando un picking deja el stock bajo

- **Estado:** aceptada
- **Fecha:** 2026-10-29
- **Autor:** Eric Vargas (revisa: Andrey Espitia)

## Contexto / problema
En la ejecución del 29 de octubre de 2026 (unos 8 minutos, 8 tiendas, producer a 5 pedidos/s) se enviaron 6.491 eventos (+120 duplicados a propósito). Al revisar el data lake vimos que la alerta `STOCK_CRITICO` dejó de ser útil:

- Se generaron **4.245 alertas de STOCK_CRITICO** sobre **6.168 eventos procesados**, es decir, cerca del **69 %** de los eventos disparó una alerta.
- **3.727** fueron de severidad ALTA (stock en 0). Casi todas vienen de eventos `ITEM_OUT_OF_STOCK`: el producto **ya estaba en 0** y cada nuevo pedido de ese SKU volvía a generar la misma alerta.
- El indicador global `pct_stockout` fue del **55 %**, y ese quiebre ya queda medido en el KPI `stockout_rate` de la ventana de 5 minutos.

Con tantas alertas repetidas, un operario no podría distinguir el aviso importante ("este producto se está acabando") del ruido.

## Opciones consideradas
1. **Bajar el umbral** `STOCK_CRITICO_MAX` de 5 a 2. Solo quita parte de las 518 alertas MEDIA; las 3.727 ALTA siguen igual, así que no resuelve el problema.
2. **Alertar solo cuando un `ITEM_PICKED` deja el stock en el umbral o por debajo** (`stock_after <= 5`), y no en cada `ITEM_OUT_OF_STOCK`. La alerta avisa una vez, cuando el producto se está agotando, y los quiebres repetidos se siguen viendo en `stockout_rate`.
3. Dejarlo como está.

## Decisión tomada
Se elige la **opción 2**. En `spark/transformations.py`, `alerta_stock_critico` ahora filtra `event_type == "ITEM_PICKED"` en vez de `ITEM_PICKED` + `ITEM_OUT_OF_STOCK`. El umbral se mantiene en `STOCK_CRITICO_MAX = 5` (ALTA si queda en 0, MEDIA entre 1 y 5). La prueba `test_alerta_stock_critico` se actualizó con un caso `ITEM_OUT_OF_STOCK` que ya no debe alertar.

## Evidencia utilizada
Salida del producer al detenerlo:
```
Total enviados: 6491 (+120 duplicados a propósito)
Por tipo: {'ITEM_PICKED': 2763, 'STOCK_REPLENISHED': 201, 'ITEM_OUT_OF_STOCK': 3527}
Mensajes por partición: {0: 3323, 1: 1640, 2: 1648}
```
Salida de `spark/revisar_datalake.py`:
```
=== Registros por zona ===
raw/inventory_events: 6284
processed/inventory_events: 6168
curated/kpis_tienda_5min: 32
curated/alertas: 4530

=== Indicadores globales ===
picking_promedio_seg | picking_p95_seg | pct_stockout
               162.8 |             240 |         55.0

=== Alertas por tipo y severidad ===
PICKING_LENTO | MEDIA |  285
STOCK_CRITICO | ALTA  | 3727
STOCK_CRITICO | MEDIA |  518
```
- PR relacionado: #[número del PR de este ADR]

## Consecuencias / trade-offs
- **Se gana:** muchas menos alertas y cada una significa algo nuevo (un SKU que acaba de quedar en 5 o menos). Según los datos de esta corrida, las alertas de stock crítico bajan de unas 4.245 a cerca de 900.
- **Se pierde:** si un producto sigue en 0 durante horas, no llega una alerta por cada pedido fallido; eso hay que verlo en el KPI `stockout_rate` por tienda.
- **Riesgo que queda 1:** el generador repone muy poco (`prob_reposicion = 0.03`), y por eso el 55 % de los eventos son quiebres. En la Semana 3 se puede subir (por ejemplo a 0.15) para tener datos más realistas.
- **Riesgo que queda 2:** desbalance de particiones. Las tiendas están parejas (entre 719 y 818 eventos cada una), pero con la key `store_id` y 8 tiendas en 3 particiones, la partición 0 recibe el doble (3.170 frente a 1.554 y 1.560). Se acepta por ahora porque el volumen es bajo; si crece, se puede subir el número de particiones.
