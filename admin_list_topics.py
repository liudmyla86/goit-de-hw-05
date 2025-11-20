from confluent_kafka.admin import AdminClient
from configs import kafka_config


admin_client = AdminClient({
    "bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
    "security.protocol": kafka_config["security_protocol"],   # SASL_PLAINTEXT
    "sasl.mechanisms": kafka_config["sasl_mechanism"],        # PLAIN
    "sasl.username": kafka_config["username"],                # admin
    "sasl.password": kafka_config["password"],                # my password
})

print("\nRequesting metadata from Kafka cluster...\n")


metadata = admin_client.list_topics(timeout=10)

all_topics = metadata.topics.keys()

print("=== ALL TOPICS ON SERVER ===")
for topic in all_topics:
    print("-", topic)

print("\n=== TOPICS THAT CONTAIN 'liudmyla' ===")
filtered = [topic for topic in all_topics if "liudmyla" in topic]
for topic in filtered:
    print("-", topic)

print("\nDone.\n")
