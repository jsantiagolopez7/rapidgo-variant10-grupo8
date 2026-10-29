"""Job de Spark Structured Streaming: Kafka -> Data Lake (raw, processed, curated).

Ejecutar dentro del contenedor (ver README):
  spark-submit --master local[2] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /opt/app/spark/streaming_job.py
"""
import os

from pyspark.sql import SparkSession, functions as F

from transformations import (
    parse_events, validate_events, enrich_events, kpis_por_tienda, construir_alertas,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "rapidgo.g8.inventory.events")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
LAKE = os.getenv("DATALAKE_PATH", "/opt/app/datalake")
TRIGGER = os.getenv("TRIGGER", "10 seconds")
WATERMARK = os.getenv("WATERMARK", "1 minute")


def read_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )


def start_sink(df, name, path, partition_by=None):
    writer = (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{LAKE}/{path}")
        .option("checkpointLocation", f"{LAKE}/checkpoints/{name}")
        .trigger(processingTime=TRIGGER)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    return writer.queryName(name).start()


def main():
    spark = SparkSession.builder.appName("g8-inventory-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark)
    parsed = parse_events(kafka_df)

    # RAW: el evento tal como llegó, con partición y offset de Kafka
    raw = parsed.select("raw_json", "topic", "partition", "offset", "kafka_ts",
                        F.current_timestamp().alias("ingested_at"))
    start_sink(raw, "raw_inventory_events", "raw/inventory_events")

    # PROCESSED: validado, deduplicado y enriquecido
    processed = (
        enrich_events(validate_events(parsed))
        .withWatermark("event_ts", WATERMARK)
        .dropDuplicates(["event_id", "event_ts"])
    )
    start_sink(processed.drop("raw_json"), "processed_inventory_events",
               "processed/inventory_events", partition_by=["fecha", "store_id"])

    # CURATED: KPIs por tienda en ventanas de 5 min y alertas
    start_sink(kpis_por_tienda(processed), "curated_kpis_tienda_5min", "curated/kpis_tienda_5min")
    start_sink(construir_alertas(processed), "curated_alertas", "curated/alertas")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
