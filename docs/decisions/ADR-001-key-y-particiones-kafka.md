# ADR-001 - Topic, key y número de particiones en Kafka

## Contexto / problema
Los eventos de inventario y picking llegan desde varias tiendas. Los KPIs y las alertas se calculan por tienda, así que necesitamos que los eventos de una misma tienda lleguen en orden y que el procesamiento pueda paralelizarse.

## Opciones consideradas
1. Key `store_id` con 3 particiones.
2. Key `sku` con 3 o 6 particiones.
3. Sin key (round robin).

## Decisión tomada
Topic `rapidgo.g8.inventory.events`, key `store_id`, 3 particiones, factor de replicación 1 (entorno local con un solo broker).

## Evidencia utilizada
- Con `store_id` todos los eventos de una tienda caen en la misma partición y se conserva su orden.
- Con `sku` los SKUs más vendidos concentrarían eventos en pocas particiones y una misma tienda quedaría repartida.
- Sin key se pierde el orden por tienda.
- Topic: rapidgo.g8.inventory.events  TopicId: 8U4jsqthR2mtTCr3POYVNg PartitionCount: 3   ReplicationFactor: 1   Configs:
         Topic: rapidgo.g8.inventory.events   Partition: 0   Leader: 1   Replicas: 1  Isr: 1   Elr:  LastKnownElr:
         Topic: rapidgo.g8.inventory.events   Partition: 1   Leader: 1   Replicas: 1  Isr: 1   Elr:  LastKnownElr:
         Topic: rapidgo.g8.inventory.events   Partition: 2   Leader: 1   Replicas: 1  Isr: 1   Elr:  LastKnownElr:
  “Análisis detallado en docs/topic-rapidgo-g8-inventory-events.md”.
## Consecuencias / trade-offs
- Con pocas tiendas puede haber particiones desbalanceadas; por eso se simulan al menos 8 tiendas.
- Más de 3 particiones no aporta en local, pero se puede subir más adelante sin cambiar la key.
