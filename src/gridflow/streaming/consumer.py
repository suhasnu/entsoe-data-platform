import json
from collections.abc import Iterator
from types import TracebackType
from typing import Any

from confluent_kafka import Consumer

from gridflow.logging import get_logger

log = get_logger(__name__)


class ConsumeError(Exception):
    """The broker returned something the consumer cannot act on."""


class EventConsumer:
    """Reads a topic in batches and leaves committing to the caller.

    Auto commit runs on a timer and has no idea whether a message was handled,
    so a crash between the commit and the write loses it. Committing after the
    write turns that loss into redelivery instead, which the deterministic
    event_id makes harmless.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
        batch_size: int = 500,
        idle_seconds: float = 5.0,
    ) -> None:
        self._topic = topic
        self._batch_size = batch_size
        self._idle_seconds = idle_seconds
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )

    def __enter__(self) -> "EventConsumer":
        self._consumer.subscribe([self._topic])
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._consumer.close()

    def batches(self) -> Iterator[list[dict[str, Any]]]:
        """Yield decoded events, stopping once the topic has gone quiet."""
        batch: list[dict[str, Any]] = []
        idle = 0.0

        while True:
            message = self._consumer.poll(1.0)

            if message is None:
                if batch:
                    yield batch
                    batch = []
                    idle = 0.0
                    continue
                idle += 1.0
                if idle >= self._idle_seconds:
                    return
                continue

            if message.error():
                raise ConsumeError(str(message.error()))

            idle = 0.0
            value = message.value()
            if value is None:
                # A null value is a tombstone. Nothing to land, but the offset
                # still has to move past it.
                continue

            batch.append(json.loads(value))
            if len(batch) >= self._batch_size:
                yield batch
                batch = []

    def commit(self) -> None:
        """Synchronous on purpose: an async commit can outlive a crash."""
        self._consumer.commit(asynchronous=False)
