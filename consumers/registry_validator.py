import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import redis
from confluent_kafka import Producer, Consumer
from dotenv import load_dotenv

# make the project root importable so "lib" can be found
sys.path.append(str(Path(__file__).parent.parent))
from lib.registry_client import check_registry, typosquat_target, benign_nonexistent

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST        = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT        = int(os.getenv("REDIS_PORT", 6379))
IN_TOPIC          = "raw.dependencies"
OUT_TOPIC         = "validation.results"

REGISTRY_CACHE_TTL = 3600   # 1h — registry data changes slowly
RECENT_DAYS        = 30     # packages newer than this are flagged

# ── Clients ─────────────────────────────────────────────────────────────────

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "registry-validator",
    "auto.offset.reset": "earliest",
})

client = httpx.Client(timeout=10, follow_redirects=True)
cache  = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# ── Helpers ─────────────────────────────────────────────────────────────────

def lookup(name: str, ecosystem: str) -> dict:
    """Check a package against its registry, caching the result in Redis."""
    cache_key = f"registry:{ecosystem}:{name}"
    cached = cache.get(cache_key)
    if cached is not None:
        return json.loads(cached)

    result = check_registry(client, name, ecosystem)
    cache.set(cache_key, json.dumps(result), ex=REGISTRY_CACHE_TTL)
    return result


def is_recent(first_published: str | None) -> bool:
    """True if the package was first published within RECENT_DAYS."""
    if not first_published:
        return False
    try:
        published = datetime.fromisoformat(first_published.replace("Z", "+00:00"))
    except ValueError:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)
    return published > cutoff


def evaluate(name: str, ecosystem: str, registry: dict) -> tuple[list[str], float]:
    """Compute suspicion flags and a 0-1 score from registry data."""
    flags = []
    score = 0.0

    exists = registry.get("exists")
    recent = is_recent(registry.get("first_published"))

    # When a package is absent from the registry, distinguish a genuine
    # hallucination from an expected absence (stdlib module, import path, or a
    # monorepo-internal workspace package).
    benign = benign_nonexistent(name, ecosystem) if exists is False else None
    hallucinated = exists is False and not benign

    if hallucinated:
        flags.append("NOT_IN_REGISTRY")
        score = 0.9   # hallucinated or removed — highest signal
    elif benign:
        flags.append(f"NOT_PUBLISHABLE:{benign}")   # informational, score stays 0

    if recent:
        flags.append("RECENTLY_PUBLISHED")
        score = max(score, 0.5)

    # A typosquat is only meaningful for packages that are NOT established:
    # either they're hallucinated (registerable by an attacker) or they were
    # published very recently. Established packages that happen to be near a
    # popular name (e.g. 'sympy' vs 'numpy') are not typosquats.
    if hallucinated or recent:
        target = typosquat_target(name, ecosystem)
        if target:
            flags.append(f"TYPOSQUAT_CANDIDATE:{target}")
            score = max(score, 0.8)

    return flags, score


def delivery_report(err, msg):
    if err:
        print(f"✗ Delivery failed: {err}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    consumer.subscribe([IN_TOPIC])
    print(f"Registry validator started — consuming '{IN_TOPIC}'")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            dep = json.loads(msg.value().decode("utf-8"))
            name      = dep["package_name"]
            ecosystem = dep["ecosystem"]

            try:
                registry = lookup(name, ecosystem)
            except httpx.HTTPError as e:
                print(f"  Registry error for {ecosystem}/{name}: {e}")
                continue

            flags, score = evaluate(name, ecosystem, registry)

            out = {
                "repo":            dep.get("repo"),
                "commit_sha":      dep.get("commit_sha"),
                "ecosystem":       ecosystem,
                "package_name":    name,
                "version_spec":    dep.get("version_spec"),
                "exists":          registry.get("exists"),
                "first_published": registry.get("first_published"),
                "latest_version":  registry.get("latest_version"),
                "flags":           flags,
                "suspicion_score": score,
                "validated_at":    datetime.now(timezone.utc).isoformat(),
            }

            producer.produce(
                OUT_TOPIC,
                key=name,
                value=json.dumps(out).encode("utf-8"),
                callback=delivery_report,
            )
            producer.flush()

            if flags:
                print(f"⚑ {ecosystem}/{name} → {flags} (score {score})")
            else:
                print(f"✓ {ecosystem}/{name} ok")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
