import argparse
from datetime import date, timedelta
from pathlib import Path

from gridflow.config import get_settings
from gridflow.ingestion.entsoe import EntsoeLoadSource
from gridflow.logging import configure_logging, get_logger
from gridflow.streaming.compare import compare, read_batch_hourly, read_stream_hourly
from gridflow.streaming.consumer import EventConsumer
from gridflow.streaming.events import events_from_frame
from gridflow.streaming.landing import write_batch
from gridflow.streaming.producer import EventProducer
from gridflow.timewindows import berlin_day_window

log = get_logger(__name__)


def produce() -> int:
    parser = argparse.ArgumentParser(prog="gridflow-produce")
    # D-2 matches the batch DAG: ENTSO-E revises actuals for a day or two.
    parser.add_argument("--day", type=date.fromisoformat, default=date.today() - timedelta(days=2))
    parser.add_argument("--zones", nargs="+")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.env != "local")

    source = EntsoeLoadSource()
    producer = EventProducer(settings.kafka_bootstrap_servers)
    zones = args.zones or settings.zones

    published = 0
    for zone in zones:
        result = source.ingest(zone, args.day)
        events = events_from_frame(result.frame, source.name, ("load_mw",))
        published += producer.publish(settings.kafka_topic_load, events)

    log.info(
        "produce.summary",
        topic=settings.kafka_topic_load,
        day=str(args.day),
        zones=len(zones),
        events=published,
        partitions=producer.delivered,
    )
    return 0


def consume() -> int:
    parser = argparse.ArgumentParser(prog="gridflow-consume")
    parser.add_argument("--group")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.env != "local")

    root = Path(settings.stream_landing_path)
    group = args.group or settings.kafka_consumer_group
    written = 0

    with EventConsumer(
        settings.kafka_bootstrap_servers,
        group,
        settings.kafka_topic_load,
        batch_size=args.batch_size,
    ) as consumer:
        for batch in consumer.batches():
            written += write_batch(batch, root)
            # Only now. A commit before the write would claim work never done.
            consumer.commit()
            log.info("consume.batch", events=len(batch), written=written)

    log.info(
        "consume.summary",
        topic=settings.kafka_topic_load,
        group=group,
        events=written,
        landing=str(root),
    )
    return 0


def compare_paths() -> int:
    parser = argparse.ArgumentParser(prog="gridflow-compare")
    parser.add_argument("--day", type=date.fromisoformat, default=date.today() - timedelta(days=2))
    parser.add_argument("--zones", nargs="+")
    parser.add_argument("--tolerance", type=float, default=0.5)
    parser.add_argument("--out", type=Path, help="write the joined rows to this CSV")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.env != "local")

    start, end = berlin_day_window(args.day)
    zones = args.zones or settings.zones

    batch = read_batch_hourly(start, end, zones)
    stream = read_stream_hourly(Path(settings.stream_hourly_path))
    stream = stream[
        (stream["ts_hour_utc"] >= start)
        & (stream["ts_hour_utc"] < end)
        & (stream["zone_code"].isin(zones))
    ]

    merged, summary = compare(batch, stream, args.tolerance)

    print(f"market day {args.day}, zones {', '.join(zones)}")
    for name, value in summary.items():
        print(f"  {name:<24} {value}")

    if args.out:
        merged.to_csv(args.out, index=False)
        print(f"  joined rows written to {args.out}")

    return 0
