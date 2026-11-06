# ADR-003 - Estrategia de orquestación y recuperación ante fallos

## Contexto / problema
El job de Spark Structured Streaming escribe continuamente el Data Lake (raw, processed y curated en ventanas de 5 minutos). Faltaba una capa que, una vez al día, revisara que los datos llegaron y son confiables, consolidara los KPIs por día y tienda y dejara un reporte. Si esa capa corre sobre datos incompletos o mal formados, publica KPIs falsos sin que nadie se entere.

Preguntas que había que resolver:
1. ¿Airflow orquesta también el streaming o solo la parte batch?
2. ¿Qué pasa cuando la fuente no está lista (Spark caído, raw vacío) o llega con un esquema inválido?
3. ¿Cómo se recupera una ejecución fallida sin duplicar resultados?

## Opciones consideradas
1. **Airflow lanza y detiene el job de streaming** (por ejemplo con `SparkSubmitOperator`). El streaming es un proceso que no termina, así que una task quedaría corriendo indefinidamente y bloquearía el DAG.
2. **Streaming independiente + DAG batch diario de control y calidad** que solo lee el Data Lake.
3. **Sin validaciones previas, solo retries**: si algo falla, Airflow reintenta y ya.

## Decisión tomada
Opción 2, con validación temprana (*fail fast*) y reintentos:

- El streaming sigue corriendo por su cuenta en `spark-master`. Airflow no lo controla.
- DAG `rapidgo_g8_inventario_diario` (`@daily`, `catchup=False`, `max_active_runs=1`):
  `check_sources → validate_raw → aggregate_daily_kpis → validate_curated → generate_report`.
- **Retries**: `retries=2` con `retry_delay=30 s` y backoff exponencial (máx. 5 min). Cubren fallos transitorios, como que Spark todavía no haya escrito el primer micro-batch.
- **Fail fast**: `check_sources` falla si raw o processed no existen o están vacíos, y `validate_raw` falla si más del 5 % de los eventos no cumplen el contrato. Así el DAG se detiene antes de calcular KPIs, y las tasks siguientes quedan en `upstream_failed` en vez de producir un reporte vacío.
- **Calidad antes de publicar**: `validate_curated` revisa nulos, duplicados por (fecha, tienda), tasas en [0, 1], tiempos no negativos y tiendas con pedidos.
- **Idempotencia**: `aggregate_daily_kpis` sobrescribe `curated/kpis_diarios/kpis_diarios_<ds>.parquet` y `generate_report` sobrescribe `reporte_inventario_<ds>.md`. Repetir una ejecución (Clear en la UI) no duplica datos.
- **XCom**: cada task pasa a la siguiente las rutas y los conteos (registros de raw y processed, inválidos, ruta de KPIs, ruta del reporte), sin volver a escanear el lake.
- **Recuperación**: se corrige la causa (por ejemplo, arrancar el job de Spark) y se hace *Clear* de la ejecución fallida. Se reprocesa el mismo `ds`, sin crear una ejecución nueva.

## Evidencia utilizada
- Ejecución de prueba del 05/11/2026 con el pipeline completo en Docker: 2.044 eventos en raw (0 inválidos), 1.944 eventos de picking en 8 tiendas; el DAG terminó en verde en unos 16 segundos y generó el reporte con stockout global de 24,69 % y picking promedio de 165,5 s.
- Fallo controlado: con el Data Lake vacío, `check_sources` falló en sus 3 intentos con `ErrorCalidad: La fuente /opt/airflow/datalake/raw/inventory_events no existe. ¿Corrió el job de Spark?` y las 4 tasks siguientes quedaron en `upstream_failed`. Ver [semana3.md](../checkpoints/semana3.md) para la ejecución del equipo.
- El CI (job `validar-dag`) corre el DAG completo con `dag.test()` sobre un Data Lake de prueba y verifica también que falle con raw vacío.

## Consecuencias / trade-offs
- (+) Los KPIs diarios solo se publican cuando la fuente y el resultado pasan las reglas de calidad.
- (+) Los fallos transitorios se resuelven solos con los reintentos y los permanentes quedan visibles en rojo con un mensaje claro.
- (−) Airflow no reinicia el streaming si se cae; eso se detecta al día siguiente en `check_sources`. Un sensor o una alerta de salud del job queda como mejora para la Semana 4.
- (−) Con los reintentos y el backoff, un fallo permanente tarda unos 2 minutos en marcarse como `failed`.
- (−) `aggregate_daily_kpis` lee todo `processed/` con pandas. Sirve para el volumen del proyecto (miles de eventos); con millones habría que filtrar por la partición `fecha` o pasar esa task a Spark.
