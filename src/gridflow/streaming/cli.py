import argparse
import sys
from datetime import date, timedelta

from gridflow.config import get_settings
from gridflow.ingestion.entsoe import EntsoeLoadSource
from gridflow.logging import configure_logging, get_logger
from gridflow.streaming.events import events_from_frame
from gridflow.streaming.producer import EventProducer

log = get_logger(__name__)


def main() -> int:
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


if __name__ == "__main__":
    sys.exit(main())
