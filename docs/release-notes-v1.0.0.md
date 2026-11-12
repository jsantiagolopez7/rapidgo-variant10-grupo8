## RapidGo · Variante 10 · Inventario y fulfillment · Grupo 8

Versión final del proyecto: pipeline completo de eventos de inventario y picking, con quality gate en GitHub Actions.

**Video final:** <ENLACE AL VIDEO>

### Qué incluye
- **Ingesta:** producer de eventos de inventario y picking con key `store_id`, topic `rapidgo.g8.inventory.events` (3 particiones, Kafka 3.9 en KRaft).
- **Procesamiento:** Spark 3.5 Structured Streaming con validación del contrato, deduplicación por `event_id`, ventanas de 5 minutos por tienda y 2 alertas (stock crítico y picking fuera de tiempo).
- **Data Lake:** zonas raw, processed y curated en Parquet, con checkpoints de streaming.
- **Orquestación:** Airflow 2.10 con el DAG diario `rapidgo_g8_inventario_diario` (5 tasks, 2 retries con backoff, XCom, reglas de calidad en raw y curated, KPIs diarios por tienda y reporte en Markdown).
- **Calidad:** CI en cada PR (`CI RapidGo` y `CI DAG Airflow`) y workflow `Release RapidGo`, que solo publica el release si todas las pruebas pasan.
- **Documentación:** README final con instalación, arquitectura, resultados y tabla de contribuciones; ADR-001 a ADR-003; checkpoints de las Semanas 1 a 3.

### Novedades de la Semana 4
- Workflow `release.yml` (quality gate + publicación del release con `workflow_dispatch`).
- README final y corrección de la documentación del CI (ADR-003).
- Integración end-to-end verificada desde cero siguiendo el README: Spark con 4 particiones de shuffle (lotes de 623 s a 30-60 s) y Kafka persistente en su volumen.
- Cierre o justificación de todos los Issues del tablero.

### PRs de la Semana 4
- #34 Integración y estabilización end-to-end (Jhonatan Santiago López) · closes #18, #17
- #33 Quality gate y release.yml (Andrey Espitia) · closes #19
- README final y tabla de contribuciones (Eric Vargas) · closes #20, #16

### Releases anteriores
v0.1.0-foundation · v0.2.0-streaming · v0.3.0-orchestration
