import os
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

topics = [
    NewTopic("github.push.events",  num_partitions=3, replication_factor=1),
    NewTopic("raw.dependencies",    num_partitions=6, replication_factor=1),
    NewTopic("validation.results",  num_partitions=6, replication_factor=1),
]

results = admin.create_topics(topics)

for topic, future in results.items():
    try:
        future.result()
        print(f"✓ Topic '{topic}' created successfully")
    except Exception as e:
        print(f"✗ Failed to create topic '{topic}': {e}")