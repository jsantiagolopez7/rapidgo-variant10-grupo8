# RapidGo · Variante 10 · Inventario y fulfillment

Proyecto final de Big Data (Línea de Profundización I). Grupo 8.

| Integrante | Rol en la Semana 1 |
|---|---|
| Jhonatan Santiago López | Repositorio, Kanban y estructura |
| Andrey Espitia | Arquitectura, contrato JSON y KPIs |
| Eric Vargas | docker-compose, README y ADR-001 |

## Problema de negocio
Los comercios aliados de RapidGo sufren agotados (stockouts) y demoras en el picking, lo que genera pedidos incompletos y retrasos en la entrega. Este proyecto construye un pipeline de datos que detecta en tiempo casi real el stock crítico y el picking fuera de tiempo, y calcula KPIs por tienda.

## Arquitectura
Producer → Kafka → Spark Structured Streaming → Data Lake (Raw → Processed → Curated) → Airflow → KPIs y alertas. Ver [docs/arquitectura.md](docs/arquitectura.md).

## Kafka
- Topic: `rapidgo.g8.inventory.events`
- Key: `store_id`
- Particiones: 3 (justificación en [ADR-001](docs/decisions/ADR-001-key-y-particiones-kafka.md))

## Evento
Contrato en [samples/evento_inventario.schema.json](samples/evento_inventario.schema.json) y ejemplo en [samples/evento_inventario.json](samples/evento_inventario.json).

## KPIs
- Tasa de stockout por tienda
- Tiempo promedio de picking (y p95)
- Porcentaje de pedidos incompletos

## Alertas
- Stock crítico: `stock_after <= 5`
- Picking fuera de tiempo: más de 8 minutos por pedido

## Ejecución local

Servicios del `docker-compose.yml`:

- **kafka**: Kafka 3.9.0 en modo KRaft (sin Zookeeper)
- **kafka-init**: crea el topic `rapidgo.g8.inventory.events` (3 particiones) y termina
- **spark-master**: Spark 3.5.3 master (UI en http://localhost:8080)

```bash
docker compose config -q     # valida (sin salida = OK)
docker compose up -d
docker compose ps -a         # kafka-init debe quedar en "Exited (0)"
docker compose logs kafka-init
```

### Verificar el topic
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic rapidgo.g8.inventory.events
```
Debe mostrar `PartitionCount: 3`.

### Conexión
- Desde tu máquina: `localhost:29092`
- Desde otros contenedores: `kafka:9092`
- Spark master: `spark://localhost:7077`

### Apagar
```bash
docker compose down        # conserva datos
docker compose down -v     # borra volúmenes
```

## Pipeline de streaming (Semana 2)

Necesitas dos terminales, ambas en la carpeta del proyecto.

**Terminal 1: levantar todo y arrancar el job de Spark**
```bash
docker compose up -d
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /opt/app/spark/streaming_job.py
```
La primera vez descarga el conector de Kafka (1 o 2 minutos). Déjalo corriendo.

**Terminal 2: enviar eventos**
```bash
docker compose run --rm producer --max-events 0 --rate 5   # continuo, Ctrl+C para parar
```
Los KPIs por ventana de 5 minutos aparecen cuando la ventana cierra (5 min + 1 min de watermark), así que deja el producer corriendo al menos 8 minutos.

**Revisar el Data Lake**
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/spark/revisar_datalake.py
```
Los archivos Parquet quedan en `datalake/raw`, `datalake/processed` y `datalake/curated`.

## Pruebas
```bash
pip install -r requirements.txt
pytest -v
```
GitHub Actions ejecuta las mismas pruebas en cada Pull Request (`.github/workflows/ci.yml`).

## Estado
- [x] v0.1.0-foundation: diseño y organización
- [x] v0.2.0-streaming: Kafka + Spark + Data Lake + CI
- [ ] v0.3.0-orchestration
- [ ] v1.0.0