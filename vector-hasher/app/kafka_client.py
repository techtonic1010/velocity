import json

from kafka import KafkaConsumer, KafkaProducer

from app import db
from app.cache import LRUCache

CONSUMER_GROUP_ID = "vector-hasher-neighbor-index-consumer"


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_neighbor_update(
    producer: KafkaProducer,
    topic: str,
    entity_id: str,
    neighbors: list[dict[str, object]],
) -> None:
    # .get() blocks until the broker acknowledges (or raises on failure) —
    # keeps this genuinely synchronous instead of a fire-and-forget send.
    future = producer.send(topic, key=entity_id, value=neighbors)
    future.get(timeout=10)


def run_consumer(dsn: str, cache: LRUCache, topic: str, bootstrap_servers: str) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=CONSUMER_GROUP_ID,
        # earliest, not the kafka-python default of latest — a message
        # produced right before this thread finishes starting up must
        # still be seen, not silently skipped.
        auto_offset_reset="earliest",
        key_deserializer=lambda key: key.decode("utf-8"),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    for message in consumer:
        entity_id = message.key
        neighbors = message.value
        db.save_neighbors_read(dsn, entity_id, neighbors)
        cache.invalidate(entity_id)
