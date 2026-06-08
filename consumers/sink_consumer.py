import os
import sys
import json
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PG_HOST           = os.getenv("PG_HOST", "localhost")
PG_PORT           = int(os.getenv("PG_PORT", 5432))
PG_USER           = os.getenv("PG_USER", "deptracker")
PG_PASSWORD       = os.getenv("PG_PASSWORD", "deptracker")
PG_DB             = os.getenv("PG_DB", "deptracker")
IN_TOPIC          = "validation.results"

SCHEMA_PATH = Path(__file__).parent.parent / "infra" / "schema.sql"

# ── Clients ─────────────────────────────────────────────────────────────────

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "sink-consumer",
    "auto.offset.reset": "earliest",
})

conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB,
)
conn.autocommit = True

# ── SQL ───────────────────────────────────────────────────────────────────────

INSERT_OBSERVATION = """
    INSERT INTO dependency_observations
        (repo, commit_sha, ecosystem, package_name, version_spec,
         exists_in_reg, suspicion_score, flags, validated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    ON CONFLICT (repo, commit_sha, ecosystem, package_name) DO NOTHING
    RETURNING id;
"""

UPSERT_PACKAGE = """
    INSERT INTO packages
        (ecosystem, package_name, exists_in_reg, first_published,
         latest_version, max_suspicion, observation_count, last_seen)
    VALUES (%s, %s, %s, %s, %s, %s, 1, %s)
    ON CONFLICT (ecosystem, package_name) DO UPDATE SET
        exists_in_reg     = EXCLUDED.exists_in_reg,
        first_published   = COALESCE(EXCLUDED.first_published, packages.first_published),
        latest_version    = EXCLUDED.latest_version,
        max_suspicion     = GREATEST(packages.max_suspicion, EXCLUDED.max_suspicion),
        observation_count = packages.observation_count + 1,
        last_seen         = EXCLUDED.last_seen;
"""

# ── Helpers ─────────────────────────────────────────────────────────────────

def init_schema():
    """Run schema.sql so tables exist before we consume."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text())
    print("✓ Schema ready")


def ts(value):
    """Normalize empty timestamps to NULL."""
    return value if value else None


def persist(record: dict) -> bool:
    """Insert one validation result. Returns True if it was a new observation."""
    with conn.cursor() as cur:
        cur.execute(INSERT_OBSERVATION, (
            record.get("repo"),
            record.get("commit_sha"),
            record["ecosystem"],
            record["package_name"],
            record.get("version_spec"),
            record.get("exists"),
            record.get("suspicion_score"),
            json.dumps(record.get("flags", [])),
            ts(record.get("validated_at")),
        ))
        inserted = cur.fetchone() is not None

        # only update the canonical table for genuinely new observations,
        # so redelivered messages don't inflate observation_count
        if inserted:
            cur.execute(UPSERT_PACKAGE, (
                record["ecosystem"],
                record["package_name"],
                record.get("exists"),
                ts(record.get("first_published")),
                record.get("latest_version"),
                record.get("suspicion_score"),
                ts(record.get("validated_at")),
            ))
    return inserted


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    init_schema()
    consumer.subscribe([IN_TOPIC])
    print(f"Sink consumer started — consuming '{IN_TOPIC}'")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            record = json.loads(msg.value().decode("utf-8"))
            try:
                is_new = persist(record)
            except Exception as e:
                print(f"  DB error for {record.get('package_name')}: {e}")
                continue

            tag = "＋" if is_new else "·"
            print(f"{tag} {record['ecosystem']}/{record['package_name']}")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run()
