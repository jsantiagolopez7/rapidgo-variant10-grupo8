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
- **airflow**: Airflow 2.10.5 en modo standalone (UI en http://localhost:8081, usuario `admin`, contraseña `admin`)

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

## Orquestación con Airflow (Semana 3)

El streaming sigue corriendo en `spark-master`. Airflow corre una vez al día el DAG `rapidgo_g8_inventario_diario`, que lee el Data Lake ([ADR-003](docs/decisions/ADR-003-orquestacion-y-recuperacion.md)):

```
check_sources -> validate_raw -> aggregate_daily_kpis -> validate_curated -> generate_report
```

| Task | Qué hace | XCom que devuelve |
|---|---|---|
| `check_sources` | raw y processed existen y tienen registros | rutas y conteos |
| `validate_raw` | máximo 5 % de eventos fuera del contrato | total, válidos, inválidos |
| `aggregate_daily_kpis` | KPIs por día y tienda → `datalake/curated/kpis_diarios/` | ruta y filas |
| `validate_curated` | nulos, duplicados, tasas en [0, 1], tiempos ≥ 0 | resumen |
| `generate_report` | reporte Markdown → `airflow/reports/` | ruta del reporte |

Todas las tasks tienen 2 retries con backoff exponencial. El código está en `airflow/dags/` y las reglas de calidad en `airflow/dags/calidad_inventario.py`.

**Ejecutar el DAG** (con el pipeline de streaming corriendo unos minutos):
1. Abre http://localhost:8081 (usuario `admin`, contraseña `admin`).
2. Activa el DAG `rapidgo_g8_inventario_diario` con el interruptor de la izquierda. Al activarlo, Airflow también crea la ejecución programada del día anterior.
3. Pulsa ▶ (*Trigger DAG*) y mira la vista *Grid*: las 5 tasks deben quedar en verde.

Desde la terminal:
```bash
docker compose exec airflow airflow dags unpause rapidgo_g8_inventario_diario
docker compose exec airflow airflow dags trigger rapidgo_g8_inventario_diario
docker compose exec airflow airflow dags list-runs -d rapidgo_g8_inventario_diario
```

**Fallo controlado:** con el Data Lake vacío (o disparando el DAG con la configuración `{"raw_subdir": "raw/vacio"}`), `check_sources` falla después de 3 intentos y las demás tasks quedan en `upstream_failed`. Se recupera corrigiendo la causa y haciendo *Clear* de la ejecución. Ver [docs/checkpoints/semana3.md](docs/checkpoints/semana3.md).

## Pruebas
```bash
pip install -r requirements.txt
pytest -v
```
GitHub Actions ejecuta en cada Pull Request (`.github/workflows/ci.yml`):
- **validar**: sintaxis, pruebas de contrato, Spark y reglas de calidad, y `docker compose config`.
- **validar-dag**: instala Airflow, importa el DAG sin errores y lo corre completo con `dag.test()` sobre un Data Lake de prueba, incluido el fallo con raw vacío (`tests/test_dag_airflow.py`).

## Estado
- [x] v0.1.0-foundation: diseño y organización
- [x] v0.2.0-streaming: Kafka + Spark + Data Lake + CI
- [x] v0.3.0-orchestration: Airflow + calidad + CI ampliado
- [ ] v1.0.0