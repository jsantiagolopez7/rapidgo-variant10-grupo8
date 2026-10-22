from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kafka-demo").getOrCreate()

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "rapidgo.g8.inventory.events")
      .option("startingOffsets", "earliest")
      .load())

query = (df.selectExpr("CAST(value AS STRING) AS value")
         .writeStream.format("console").outputMode("append").start())
query.awaitTermination()
