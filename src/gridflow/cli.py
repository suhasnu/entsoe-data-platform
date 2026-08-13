import argparse
import sys
from datetime import date, timedelta

from gridflow.config import get_settings
from gridflow.ingestion.entsoe import (
    EntsoeGenerationSource,
    EntsoeLoadSource,
    EntsoePriceSource,
)
from gridflow.logging import configure_logging, get_logger
from gridflow.storage.writers import load_to_bronze

log = get_logger(__name__)

SOURCES = {
    "load": (EntsoeLoadSource, "raw_load"),
    "generation": (EntsoeGenerationSource, "raw_generation"),
    "price": (EntsoePriceSource, "raw_day_ahead_price"),
}


def ingest_day(day: date, zones: list[str], sources: list[str]) -> int:
    failures = 0
    for source_name in sources:
        source_cls, table = SOURCES[source_name]
        source = source_cls()
        for zone in zones:
            try:
                result = source.ingest(zone, day)
                load_to_bronze(result.frame, table)
            except Exception as exc:
                # One bad zone should not stop the other five.
                failures += 1
                log.error("ingest.failed", source=source_name, zone=zone,
                          day=str(day), error=str(exc))
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(prog="gridflow")
    parser.add_argument("--day", type=date.fromisoformat,
                        default=date.today() - timedelta(days=1))
    parser.add_argument("--zones", nargs="+")
    parser.add_argument("--sources", nargs="+", choices=list(SOURCES),
                        default=list(SOURCES))
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.env != "local")

    zones = args.zones or settings.zones
    failures = ingest_day(args.day, zones, args.sources)

    log.info("ingest.summary", day=str(args.day), zones=len(zones),
             sources=len(args.sources), failures=failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())