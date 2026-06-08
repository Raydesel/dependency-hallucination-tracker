import os
import json
from confluent_kafka import Producer, Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "github.push.events"

# ── Producer ──────────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err:
        print(f"✗ Delivery failed: {err}")
    else:
        print(f"✓ Message delivered to '{msg.topic()}' [partition {msg.partition()}] @ offset {msg.offset()}")

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

test_message = {
    "repo": "test-owner/test-repo",
    "commit_sha": "abc123def456",
    "pushed_at": "2026-06-06T10:00:00Z",
    "files_changed": ["requirements.txt", "src/main.py"]
}

print("--- Producing test message ---")
producer.produce(
    TOPIC,
    key="test-owner/test-repo",
    value=json.dumps(test_message).encode("utf-8"),
    callback=delivery_report
)
producer.flush()

# ── Consumer ──────────────────────────────────────────────────────────────────

print("\n--- Consuming test message ---")
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "smoke-test-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

try:
    msg = consumer.poll(timeout=10.0)
    if msg is None:
        print("✗ No message received within timeout")
    elif msg.error():
        print(f"✗ Consumer error: {msg.error()}")
    else:
        payload = json.loads(msg.value().decode("utf-8"))
        print(f"✓ Message consumed successfully:")
        print(f"  repo:         {payload['repo']}")
        print(f"  commit_sha:   {payload['commit_sha']}")
        print(f"  files_changed:{payload['files_changed']}")
finally:
    consumer.close()
    print("\nSmoke test complete.")