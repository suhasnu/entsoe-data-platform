import argparse
import os
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("zone_code", StringType()),
        StructField("ts_utc", StringType()),
        StructField("produced_at", StringType()),
        StructField("resolution_minutes", IntegerType()),
        StructField("payload", StructType([StructField("load_mw", DoubleType())])),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="hourly_load")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC_LOAD", "entsoe.load.raw"))
    # Generous by streaming standards, because a poll publishes a whole market
    # day at once and the watermark jumps to its end the moment it arrives.
    parser.add_argument("--watermark", default="2 hours")
    parser.add_argument("--output", default="data/stream/hourly")
    parser.add_argument("--checkpoint", default="data/stream/_checkpoints/hourly")
    parser.add_argument("--continuous", action="store_true")
    return parser.parse_args()


def report(query: StreamingQuery) -> None:
    progress: list[dict[str, Any]] = list(query.recentProgress)
    rows_in = sum(p.get("numInputRows", 0) for p in progress)
    dropped = sum(
        op.get("numRowsDroppedByWatermark", 0)
        for p in progress
        for op in p.get("stateOperators", [])
    )
    held = 0
    if progress and progress[-1].get("stateOperators"):
        held = progress[-1]["stateOperators"][-1].get("numRowsTotal", 0)

    print(f"batches:              {len(progress)}")
    print(f"input rows:           {rows_in}")
    print(f"dropped by watermark: {dropped}")
    print(f"still held in state:  {held}")


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("gridflow-hourly-load").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        # Only used on the first run. After that the checkpoint decides, and
        # Spark tracks offsets there rather than in a Kafka consumer group.
        .option("startingOffsets", "earliest")
        .load()
    )

    events = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.zone_code").alias("zone_code"),
        F.to_timestamp("e.ts_utc").alias("ts_utc"),
        F.col("e.payload.load_mw").alias("load_mw"),
    )

    hourly = (
        events.withWatermark("ts_utc", args.watermark)
        # The topic carries redeliveries under the same id. They do not move the
        # average, since the values are identical, but they do inflate the count.
        .dropDuplicates(["event_id", "ts_utc"])
        .groupBy(F.window("ts_utc", "1 hour"), "zone_code")
        .agg(
            # Average, not sum. Load is a rate in MW, same as the dbt staging layer.
            F.avg("load_mw").alias("load_mw"),
            # Counts the column, not the row, so a null interval is excluded the
            # way stg_load excludes it. Otherwise the two paths disagree on count
            # while agreeing on value, which reads as a bug and is not one.
            F.count("load_mw").alias("intervals"),
        )
        .select(
            F.col("window.start").alias("ts_hour_utc"),
            "zone_code",
            "load_mw",
            "intervals",
        )
    )

    writer = (
        hourly.writeStream.format("parquet")
        # A file sink supports append only, so the watermark alone decides when
        # a window is final and gets written.
        .outputMode("append")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("zone_code")
    )

    query = (
        writer.trigger(processingTime="30 seconds").start()
        if args.continuous
        else writer.trigger(availableNow=True).start()
    )
    query.awaitTermination()
    report(query)


if __name__ == "__main__":
    main()
