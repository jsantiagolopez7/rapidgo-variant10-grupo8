# RapidGo G8 – Kafka 3.9 (KRaft) + Spark 3.5.3

docker-compose.yml preliminar:

- **kafka**: Kafka 3.9.0 en modo KRaft (sin Zookeeper)
- **kafka-init**: crea el topic `rapidgo.g8.inventory.events` (3 particiones) y termina
- **spark-master**: Spark 3.5.3 master (UI en http://localhost:8080)

## Uso
```bash
docker compose config -q     # valida (sin salida = OK)
docker compose up -d
docker compose ps -a         # kafka-init debe quedar en "Exited (0)"
docker compose logs kafka-init
```

## Verificar el topic
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic rapidgo.g8.inventory.events
```
Debe mostrar `PartitionCount: 3`.

## Conexión
- Desde tu máquina: `localhost:29092`
- Desde otros contenedores: `kafka:9092`
- Spark master: `spark://localhost:7077`

## Apagar
```bash
docker compose down        # conserva datos
docker compose down -v     # borra volúmenes
```
