from kafka.admin import KafkaAdminClient
from kafka_config import kafka_config
import sys

# Topics to be deleted (з правильними назвами)
TOPICS = [
    "building_sensors_hellcat_topic",
    "temperature_alerts_hellcat_topic",
    "humidity_alerts_hellcat_topic",
    "alerts_hellcat_topic"
]

def delete_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password']
        )
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)

    try:
        existing_topics = admin_client.list_topics()
        topics_to_delete = [topic for topic in TOPICS if topic in existing_topics]

        if not topics_to_delete:
            print("⚠️ No topics to delete.")
            return

        admin_client.delete_topics(topics=topics_to_delete)
        print(f"✅ Deleted topics: {topics_to_delete}")
    except Exception as e:
        print(f"❌ Error while deleting topics: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    delete_topics()
