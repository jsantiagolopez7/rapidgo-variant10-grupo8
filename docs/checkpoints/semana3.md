# Checkpoint Semana 3

1. **Qué quedó funcionando esta semana:** Airflow 2.10 en el `docker-compose.yml` (UI en http://localhost:8081) con el DAG `rapidgo_g8_inventario_diario` de 5 tasks: `check_sources → validate_raw → aggregate_daily_kpis → validate_curated → generate_report`. Tiene retries (2, con backoff exponencial), XCom con conteos y rutas, reglas de calidad sobre raw y curated, KPIs diarios por tienda en `datalake/curated/kpis_diarios/` y un reporte en `airflow/reports/`. El CI tiene un job nuevo, `validar-dag`, que importa el DAG, lo corre completo con datos de prueba y verifica el fallo controlado.

2. **Ejecución correcta (valor real):** <!-- ERIC: completar con la corrida exitosa: fecha, eventos en raw, inválidos, tiendas, stockout global y picking promedio (salen en el log de generate_report) -->
   Ejecución `____` del __/11/2026: ___ eventos en raw (___ inválidos), ___ eventos de picking en ___ tiendas, stockout global ___ %, picking promedio ___ s.

3. **Tarea que falló:** `check_sources` (ejecución `____`).
   - **Cómo se provocó:** <!-- ERIC: p. ej. "se borró el Data Lake y se disparó el DAG antes de arrancar el job de Spark" -->
   - **Mensaje real del log** (`airflow/logs/dag_id=rapidgo_g8_inventario_diario/run_id=.../task_id=check_sources/attempt=3.log`):
     ```
     <!-- ERIC: pegar aquí las líneas ERROR / ErrorCalidad / Marking task as FAILED del log -->
     ```
   - **Intentos:** 3 (1 + 2 retries); los dos primeros quedaron en `up_for_retry`.

4. **Tareas afectadas:** `validate_raw`, `aggregate_daily_kpis`, `validate_curated` y `generate_report` quedaron en `upstream_failed`. No se escribieron KPIs diarios ni reporte para esa ejecución, que es lo que buscábamos: no publicar KPIs sobre datos vacíos.

5. **Cambio que resolvió el problema:** <!-- ERIC: p. ej. "se arrancó el job de Spark y el producer, Spark escribió raw y processed, y se hizo Clear de la ejecución fallida; las 5 tasks terminaron en success" -->
   Además, al montar Airflow en Linux falló con `ValueError: Unable to configure handler 'processor'` porque el usuario `airflow` (uid 50000) no podía escribir en `./airflow/logs`, creada por Docker como root. Se resolvió con `user: "0:0"` en el servicio `airflow` (PR #__).

6. **Decisión técnica:** [ADR-003](../decisions/ADR-003-orquestacion-y-recuperacion.md): el streaming corre aparte y Airflow orquesta un DAG batch diario de control y calidad con *fail fast*, retries e idempotencia.

7. **Enlaces:** PR #__ (DAG y Airflow, Jhonatan), PR #__ (CI ampliado, Andrey), PR #__ (fallo controlado y checkpoint, Eric), Action CI RapidGo #__ en verde, release v0.3.0-orchestration.
