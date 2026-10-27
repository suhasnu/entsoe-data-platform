from collections import Counter
from collections.abc import Sequence
from typing import Any

from confluent_kafka import Producer

from gridflow.logging import get_logger
from gridflow.streaming.events import GridEvent

log = get_logger(__name__)


class DeliveryError(Exception):
    """Events left the process but never reached the broker."""


class EventProducer:
    """Kafka producer that will not lose messages quietly.

    Idempotence pins acks to all, bounds in flight requests and tags each batch
    with a sequence number, so a retry after a timeout cannot duplicate a record
    or reorder a partition. Without it the default retry behaviour can do both.
    """

    def __init__(self, bootstrap_servers: str, client_id: str = "gridflow-producer") -> None:
        self._errors: list[str] = []
        self._delivered: Counter[int] = Counter()
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 50,
                "batch.size": 65536,
                "delivery.timeout.ms": 120_000,
            }
        )

    @property
    def delivered(self) -> dict[int, int]:
        """Events acknowledged per partition. Proves the keying did something."""
        return dict(sorted(self._delivered.items()))

    def _on_delivery(self, err: object, msg: Any) -> None:
        if err is not None:
            self._errors.append(str(err))
        else:
            self._delivered[msg.partition()] += 1

    def publish(self, topic: str, events: Sequence[GridEvent]) -> int:
        for event in events:
            while True:
                try:
                    self._producer.produce(
                        topic=topic,
                        key=event.key,
                        value=event.to_json(),
                        on_delivery=self._on_delivery,
                    )
                    break
                except BufferError:
                    # The local queue is full. Serving callbacks drains it.
                    self._producer.poll(1.0)
            self._producer.poll(0)

        unsent = self._producer.flush(60.0)
        if unsent:
            raise DeliveryError(f"{unsent} events still queued after flush")
        if self._errors:
            raise DeliveryError(f"{len(self._errors)} deliveries failed, first: {self._errors[0]}")

        log.info("produce.flushed", topic=topic, events=len(events), partitions=self.delivered)
        return len(events)
