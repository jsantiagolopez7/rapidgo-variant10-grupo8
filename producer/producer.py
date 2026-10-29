"""Producer de Kafka: envía eventos de inventario al topic con key = store_id.

Ejemplos:
  docker compose run --rm producer                      # 500 eventos, 5 por segundo
  docker compose run --rm producer --max-events 2000 --rate 20
  python producer/producer.py --bootstrap localhost:29092   # desde tu máquina
"""
import argparse
import json
import os
import time
from collections import Counter

from kafka import KafkaProducer

from generador import GeneradorInventario


def parse_args():
    p = argparse.ArgumentParser(description="Producer de eventos de inventario - Grupo 8")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "rapidgo.g8.inventory.events"))
    p.add_argument("--rate", type=float, default=5.0, help="eventos por segundo")
    p.add_argument("--max-events", type=int, default=500, help="0 = sin límite")
    p.add_argument("--tiendas", type=int, default=8)
    p.add_argument("--duplicados", type=float, default=0.02,
                   help="probabilidad de reenviar un evento (prueba la deduplicación en Spark)")
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    gen = GeneradorInventario(n_tiendas=args.tiendas, seed=args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    por_particion, por_tipo, enviados, duplicados = Counter(), Counter(), 0, 0
    print(f"Enviando a {args.topic} en {args.bootstrap} ({args.rate} eventos/s)...")
    try:
        while args.max_events == 0 or enviados < args.max_events:
            for ev in gen.pedido():
                repeticiones = 2 if gen.rng.random() < args.duplicados else 1
                for _ in range(repeticiones):
                    meta = producer.send(args.topic, key=ev["store_id"], value=ev).get(timeout=10)
                    por_particion[meta.partition] += 1
                enviados += 1
                duplicados += repeticiones - 1
                por_tipo[ev["event_type"]] += 1
                if enviados % 100 == 0:
                    print(f"  {enviados} eventos enviados")
                time.sleep(1 / args.rate)
                if args.max_events and enviados >= args.max_events:
                    break
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
    print(f"\nTotal enviados: {enviados} (+{duplicados} duplicados a propósito)")
    print("Por tipo:", dict(por_tipo))
    print("Mensajes por partición:", dict(sorted(por_particion.items())))


if __name__ == "__main__":
    main()
