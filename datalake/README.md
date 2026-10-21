# Data Lake - Grupo 8

| Zona | Ruta | Contenido | Formato |
|---|---|---|---|
| Raw | `datalake/raw/inventory_events/` | Eventos tal como llegan de Kafka, con offset y partición | Parquet |
| Processed | `datalake/processed/inventory_events/` | Eventos validados, deduplicados por `event_id` y enriquecidos (duración de picking, flag de stockout, flag de pedido incompleto). Particionado por `fecha` y `store_id` | Parquet |
| Curated | `datalake/curated/kpis_tienda_5min/` y `datalake/curated/alertas/` | KPIs por tienda en ventanas de 5 minutos y alertas generadas | Parquet |

Los checkpoints de Spark Structured Streaming se guardan en `datalake/checkpoints/`. Ninguna de estas carpetas se versiona en Git.
