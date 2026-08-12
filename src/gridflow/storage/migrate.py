from pathlib import Path

from google.cloud import bigquery

from gridflow.config import get_settings
from gridflow.logging import configure_logging, get_logger

log = get_logger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "migrations"


def run_migrations() -> None:
    settings = get_settings()
    client = bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)

    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        log.info("migration.apply", file=path.name)
        # Statements are split because BigQuery runs one per job.
        sql = path.read_text().format(project=settings.gcp_project_id)
        for statement in (s.strip() for s in sql.split(";") if s.strip()):
            client.query(statement).result()

    log.info("migration.done")


if __name__ == "__main__":
    configure_logging("INFO", json_output=False)
    run_migrations()