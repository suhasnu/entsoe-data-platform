import logging
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import cast

import structlog


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    """Set up structlog. JSON in containers, readable console locally."""
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)

    renderer = (
        structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))


@contextmanager
def run_context(dag_id: str, task_id: str, **extra: object) -> Iterator[uuid.UUID]:
    """Bind a run_id to every log line in the block.

    The same id is written to ops.pipeline_run later, so one grep over the logs
    reconstructs a whole run.
    """
    run_id = uuid.uuid4()
    structlog.contextvars.bind_contextvars(
        run_id=str(run_id), dag_id=dag_id, task_id=task_id, **extra
    )
    try:
        yield run_id
    finally:
        structlog.contextvars.clear_contextvars()
