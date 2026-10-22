# Checkpoint Semana 1

1. **Qué quedó funcionando esta semana:** repositorio, Kanban con Issues asignados, arquitectura de la variante 10, contrato JSON con ejemplo, KPIs y alertas con umbrales iniciales, docker-compose preliminar con Kafka y Spark.
2. **Decisión más importante:** key `store_id` con 3 particiones (ver ADR-001).
3. **Riesgo principal:** que Docker con Kafka y Spark consuma demasiada memoria en nuestros equipos, y generar datos sintéticos de stockouts y picking lo bastante realistas para que las alertas se disparen de forma creíble. Problema real de esta semana: subimos archivos directo a main y el PR #23 quedó con conflictos en README.md y docker-compose.yml; lo resolvimos con "Resolve conflicts" y protegimos main con un ruleset que exige PR y 1 aprobación.
4. **Plan de la Semana 2:** producer funcional, job de Spark con 3 transformaciones y ventana de 5 minutos, alertas, Parquet con checkpoint, tests y CI.
5. **Enlaces:** PR #23, Issues #1 a #8, release v0.1.0-foundation.
