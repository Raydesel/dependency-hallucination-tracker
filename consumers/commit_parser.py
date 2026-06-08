import os
import sys
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import redis
from confluent_kafka import Producer, Consumer
from dotenv import load_dotenv

# make the project root importable so "lib" can be found
sys.path.append(str(Path(__file__).parent.parent))
from lib.parsers import parse_package_json, parse_requirements_txt

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GITHUB_TOKEN      = os.getenv("GITHUB_TOKEN")
REDIS_HOST        = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT        = int(os.getenv("REDIS_PORT", 6379))
IN_TOPIC          = "github.push.events"
OUT_TOPIC         = "raw.dependencies"

FILE_CACHE_TTL    = 86400   # 24h — commit file contents are immutable
NOT_FOUND         = "__404__"
RATE_LIMIT_FLOOR  = 50      # back off when fewer than this many calls remain

# ── Clients ─────────────────────────────────────────────────────────────────

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "commit-parser",
    "auto.offset.reset": "earliest",
})

client = httpx.Client(
    headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.raw",   # get raw file content, not base64 JSON
        "X-GitHub-Api-Version": "2022-11-28",
    },
    timeout=10,
)

cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

DEP_FILES = [
    ("package.json",     parse_package_json),
    ("requirements.txt", parse_requirements_txt),
]

# ── Helpers ─────────────────────────────────────────────────────────────────

def respect_rate_limit(resp: httpx.Response) -> None:
    """Sleep until the rate-limit window resets when we're running low."""
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 1))
    if remaining < RATE_LIMIT_FLOOR:
        reset_at = int(resp.headers.get("X-RateLimit-Reset", 0))
        wait = max(0, reset_at - time.time())
        print(f"⚠ Rate limit low ({remaining} left). Sleeping {wait:.0f}s...")
        time.sleep(wait + 1)


def fetch_file(repo: str, path: str, ref: str) -> str | None:
    """Fetch a file's raw content at a specific commit, with Redis caching.

    Returns None if the file does not exist. Caches both hits and 404s so we
    never re-hit the API for the same (repo, sha, path) tuple.
    """
    cache_key = f"file:{repo}:{ref}:{path}"

    cached = cache.get(cache_key)
    if cached is not None:
        return None if cached == NOT_FOUND else cached

    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    params = {"ref": ref} if ref else None
    resp = client.get(url, params=params)

    respect_rate_limit(resp)

    if resp.status_code == 404:
        cache.set(cache_key, NOT_FOUND, ex=FILE_CACHE_TTL)
        return None

    resp.raise_for_status()
    content = resp.text
    cache.set(cache_key, content, ex=FILE_CACHE_TTL)
    return content


def delivery_report(err, msg):
    if err:
        print(f"✗ Delivery failed: {err}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    consumer.subscribe([IN_TOPIC])
    print(f"Commit parser started — consuming '{IN_TOPIC}'")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            repo = event["repo"]
            sha  = event.get("commit_sha", "")

            total = 0
            for path, parser in DEP_FILES:
                try:
                    content = fetch_file(repo, path, sha)
                except httpx.HTTPError as e:
                    print(f"  API error for {repo}/{path}: {e}")
                    continue

                if content is None:
                    continue

                for dep in parser(content):
                    out = {
                        "repo": repo,
                        "commit_sha": sha,
                        "source_file": path,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        **dep,
                    }
                    producer.produce(
                        OUT_TOPIC,
                        key=dep["package_name"],
                        value=json.dumps(out).encode("utf-8"),
                        callback=delivery_report,
                    )
                    total += 1

            if total:
                producer.flush()
                print(f"✓ {repo} @ {sha[:7]} → {total} packages")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()