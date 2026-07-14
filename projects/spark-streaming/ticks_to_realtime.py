"""
Hot path of the realtime layer: Kafka market.ticks -> 1-minute bars -> Postgres
realtime.bars_1m (upsert) + Kafka market.metrics.1m.

Per (symbol, 1-minute window) the job computes OHLC, volume, notional, VWAP,
trade count, and buy/sell volume (KBS match_type: buy / sell / ato / atc —
auction fills count toward totals but not the buy/sell split). Session VWAP
and imbalance ratios are derivable downstream (Cube/SQL) via cumulative sums
over these bars, so they are deliberately not materialized here.

Streaming mechanics:
  - 5-minute watermark on event_time; update output mode, so a bar is
    re-emitted whenever a late tick lands inside it. The Postgres sink is an
    idempotent ON CONFLICT upsert keyed by (symbol, bar_start), which makes
    replays and re-emits safe; the last write wins.
  - startingOffsets=earliest: on first start (or after a checkpoint wipe) the
    job rebuilds all bars still retained in the topic (7 days) — the hot store
    heals itself from Kafka. The checkpoint pins offsets thereafter.
  - foreachBatch collects each micro-batch to the driver for the upsert. Bar
    updates per trigger are bounded (~30 symbols x a few minutes; a full
    7-day replay is ~tens of thousands of rows), so driver-side psycopg2 is
    simpler and safer than staging tables for this volume.
"""

import json
import logging
import os
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, LongType

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ticks-to-realtime")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TICKS_TOPIC = os.environ.get("TICKS_TOPIC", "market.ticks")
METRICS_TOPIC = os.environ.get("METRICS_TOPIC", "market.metrics.1m")
PG_DSN = os.environ.get("PG_DSN", "postgresql://ngods:ngods@postgres:5432/ngods")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "s3a://warehouse/checkpoints/bars_1m")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
WATERMARK = os.environ.get("WATERMARK", "5 minutes")

TICK_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("event_time", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("match_type", StringType()),
    StructField("source", StringType()),
    StructField("ingested_at", StringType()),
])

DDL = """
CREATE SCHEMA IF NOT EXISTS realtime;
CREATE TABLE IF NOT EXISTS realtime.bars_1m (
    symbol      TEXT NOT NULL,
    bar_start   TIMESTAMPTZ NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    notional    DOUBLE PRECISION,
    vwap        DOUBLE PRECISION,
    trade_count BIGINT,
    buy_volume  BIGINT,
    sell_volume BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, bar_start)
);
"""

UPSERT_SQL = """
INSERT INTO realtime.bars_1m
    (symbol, bar_start, open, high, low, close, volume, notional, vwap,
     trade_count, buy_volume, sell_volume, updated_at)
VALUES %s
ON CONFLICT (symbol, bar_start) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    notional = EXCLUDED.notional,
    vwap = EXCLUDED.vwap,
    trade_count = EXCLUDED.trade_count,
    buy_volume = EXCLUDED.buy_volume,
    sell_volume = EXCLUDED.sell_volume,
    updated_at = now()
"""


def ensure_schema() -> None:
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
    log.info("Ensured realtime.bars_1m exists")


def upsert_bars(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (
            r["symbol"], r["bar_start"], r["open"], r["high"], r["low"], r["close"],
            r["volume"], r["notional"], r["vwap"], r["trade_count"],
            r["buy_volume"], r["sell_volume"],
        )
        for r in batch_df.collect()
        if r["symbol"] is not None
    ]
    if not rows:
        return

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                UPSERT_SQL,
                rows,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())",
                page_size=1000,
            )
    log.info("Batch %d: upserted %d bars into realtime.bars_1m", batch_id, len(rows))

    (
        batch_df
        .filter(F.col("symbol").isNotNull())
        .select(
            F.col("symbol").alias("key"),
            F.to_json(F.struct("symbol", "bar_start", "open", "high", "low", "close",
                               "volume", "notional", "vwap", "trade_count",
                               "buy_volume", "sell_volume")).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", METRICS_TOPIC)
        .save()
    )


def main() -> None:
    ensure_schema()

    spark = SparkSession.builder.appName("ticks-to-realtime").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ticks = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TICKS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("tick"))
        .select(
            F.col("tick.symbol").alias("symbol"),
            F.col("tick.event_time").cast("timestamp").alias("event_time"),
            F.col("tick.price").alias("price"),
            F.coalesce(F.col("tick.volume"), F.lit(0)).alias("volume"),
            F.lower(F.coalesce(F.col("tick.match_type"), F.lit(""))).alias("match_type"),
        )
        .filter(F.col("event_time").isNotNull() & F.col("price").isNotNull())
    )

    bars = (
        ticks
        .withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", "1 minute"), F.col("symbol"))
        .agg(
            F.min_by("price", "event_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum(F.col("price") * F.col("volume")).alias("notional"),
            F.count(F.lit(1)).alias("trade_count"),
            F.sum(F.when(F.col("match_type") == "buy", F.col("volume")).otherwise(F.lit(0))).alias("buy_volume"),
            F.sum(F.when(F.col("match_type") == "sell", F.col("volume")).otherwise(F.lit(0))).alias("sell_volume"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("bar_start"),
            "open", "high", "low", "close", "volume", "notional",
            (F.col("notional") / F.when(F.col("volume") > 0, F.col("volume"))).alias("vwap"),
            "trade_count", "buy_volume", "sell_volume",
        )
    )

    query = (
        bars.writeStream
        .outputMode("update")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(upsert_bars)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
