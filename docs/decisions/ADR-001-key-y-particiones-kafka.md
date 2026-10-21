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
- TODO (equipo): pegar aquí la salida real de `kafka-topics.sh --describe` y el conteo de mensajes por partición al correr el producer con N tiendas.

## Consecuencias / trade-offs
- Con pocas tiendas puede haber particiones desbalanceadas; por eso se simulan al menos 8 tiendas.
- Más de 3 particiones no aporta en local, pero se puede subir más adelante sin cambiar la key.
