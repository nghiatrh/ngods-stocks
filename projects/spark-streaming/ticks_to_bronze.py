"""
Cold path of the realtime layer: Kafka market.ticks -> Iceberg warehouse.bronze.ticks_stream.

Runs as a long-lived Spark Structured Streaming query submitted to the existing
standalone cluster (spark://spark:7077, hosted in the aio container). Reuses the
Iceberg JDBC catalog and S3A config already baked into /opt/spark/conf/spark-defaults.conf
in that image - this script adds no catalog config of its own.

Checkpointing at CHECKPOINT_PATH gives exactly-once delivery into the Iceberg
table (Spark tracks committed Kafka offsets per micro-batch, Iceberg commits are
atomic per batch), so a restart resumes from the last committed offset rather
than reprocessing or losing ticks. market.ticks retains 7 days, so a checkpoint
that ages out past that window would lose data - see the README's compaction
and monitoring follow-ups.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, LongType

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TICKS_TOPIC = os.environ.get("TICKS_TOPIC", "market.ticks")
BRONZE_TABLE = os.environ.get("BRONZE_TABLE", "warehouse.bronze.ticks_stream")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "s3a://warehouse/checkpoints/ticks_stream")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "30 seconds")

TICK_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("event_time", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("match_type", StringType()),
    StructField("source", StringType()),
    StructField("ingested_at", StringType()),
])


def main() -> None:
    spark = SparkSession.builder.appName("ticks-to-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            symbol         STRING,
            event_time     TIMESTAMP,
            price          DOUBLE,
            volume         BIGINT,
            match_type     STRING,
            source         STRING,
            ingested_at    TIMESTAMP,
            kafka_partition INT,
            kafka_offset   BIGINT,
            trade_date     DATE
        )
        USING iceberg
        PARTITIONED BY (trade_date)
    """)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TICKS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), TICK_SCHEMA).alias("tick"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    ).select(
        col("tick.symbol").alias("symbol"),
        col("tick.event_time").cast("timestamp").alias("event_time"),
        col("tick.price").alias("price"),
        col("tick.volume").alias("volume"),
        col("tick.match_type").alias("match_type"),
        col("tick.source").alias("source"),
        col("tick.ingested_at").cast("timestamp").alias("ingested_at"),
        col("kafka_partition"),
        col("kafka_offset"),
    ).withColumn("trade_date", to_date(col("event_time")))

    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable(BRONZE_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
