# Checkpoint Semana 3

1. **Qué quedó funcionando esta semana:** Airflow 2.10 en el `docker-compose.yml` (UI en http://localhost:8081) con el DAG `rapidgo_g8_inventario_diario` de 5 tasks: `check_sources → validate_raw → aggregate_daily_kpis → validate_curated → generate_report`. Tiene retries (2, con backoff exponencial), XCom con conteos y rutas, reglas de calidad sobre raw y curated, KPIs diarios por tienda en `datalake/curated/kpis_diarios/` y un reporte en `airflow/reports/`. El CI tiene un job nuevo, `validar-dag`, que importa el DAG, lo corre completo con datos de prueba y verifica el fallo controlado.

2. **Ejecución correcta (valor real):** ejecución `manual__2026-11-08T03:47:56+00:00` del 08/11/2026. Las 5 tasks terminaron en `success`, con 1531 eventos en raw (0 inválidos) y KPIs calculados para 8 tiendas (8 filas). Se generó el reporte `reporte_inventario_2026-11-08.md`.

3. **Tarea que falló:** `check_sources` (ejecución `manual__2026-11-08T03:42:04+00:00`). Hizo 3 intentos (1 + 2 retries); los dos primeros quedaron en `up_for_retry`.

4. **Tareas afectadas:** `validate_raw`, `aggregate_daily_kpis`, `validate_curated` y `generate_report` quedaron en `upstream_failed`. No se escribieron KPIs diarios ni reporte para esa ejecución, que es lo que buscábamos: no publicar KPIs sobre datos vacíos.

5. **Cambio que resolvió el problema:** se dejaron disponibles los datos en el Data Lake y se hizo *Clear* de la ejecución fallida. Al reintentarla, las 5 tasks terminaron en `success` (1531 eventos en raw, 8 tiendas, duración de 25 s).

6. **Decisión técnica:** [ADR-003](../decisions/ADR-003-orquestacion-y-recuperacion.md): el streaming corre aparte y Airflow orquesta un DAG batch diario de control y calidad con *fail fast*, retries e idempotencia.
