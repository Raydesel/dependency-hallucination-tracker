import os
import json
import time
import httpx
import redis
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GITHUB_TOKEN       = os.getenv("GITHUB_TOKEN")
POLL_INTERVAL      = int(os.getenv("GITHUB_POLL_INTERVAL_SECONDS", 60))
REDIS_HOST         = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT         = int(os.getenv("REDIS_PORT", 6379))
TOPIC              = "github.push.events"

# ── Clients ───────────────────────────────────────────────────────────────────

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
cache    = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
headers  = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err:
        print(f"✗ Delivery failed: {err}")
    else:
        print(f"✓ Produced → {msg.topic()} [partition {msg.partition()}]")


def already_seen(event_id: str) -> bool:
    key = f"seen:event:{event_id}"
    if cache.exists(key):
        return True
    cache.set(key, "1", ex=3600)
    return False



def fetch_public_push_events() -> list:
    url = "https://api.github.com/events?per_page=100"
    with httpx.Client(headers=headers, timeout=10) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


# ── Main loop ─────────────────────────────────────────────────────────────────

def poll():
    print(f"Poller started — checking GitHub every {POLL_INTERVAL}s")
    while True:
        try:
            events = fetch_public_push_events()
            produced = 0

            for event in events:
                if event.get("type") != "PushEvent":
                    continue

                event_id = event["id"]
                if already_seen(event_id):
                    continue

                payload = event.get("payload", {})
                message = {
                    "event_id":     event_id,
                    "repo":         event["repo"]["name"],
                    "commit_sha":   payload.get("head", ""),
                    "pushed_at":    event.get("created_at", ""),
                    "commit_count": len(payload.get("commits", [])),
                }

                producer.produce(
                    TOPIC,
                    key=message["repo"],
                    value=json.dumps(message).encode("utf-8"),
                    callback=delivery_report,
                )
                produced += 1

            producer.flush()
            print(f"Polled {len(events)} events → {produced} new dependency pushes produced")

        except httpx.HTTPError as e:
            print(f"GitHub API error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    poll()