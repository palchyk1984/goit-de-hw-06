from kafka.admin import KafkaAdminClient, NewTopic
from kafka_config import kafka_config
import sys

# Define topics to create (з правильними назвами)
TOPICS = [
    "building_sensors_hellcat_topic",  # Input topic for sensor data
    "temperature_alerts_hellcat_topic",  # Output topic for temperature alerts
    "humidity_alerts_hellcat_topic",  # Output topic for humidity alerts
    "alerts_hellcat_topic"  # General alert topic
]

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
except Exception as e:
    print(f"❌ Error connecting to Kafka: {e}")
    sys.exit(1)

# Check existing topics
existing_topics = set(admin_client.list_topics())
new_topics = [
    NewTopic(name=topic, num_partitions=3, replication_factor=1)
    for topic in TOPICS if topic not in existing_topics
]

# Create topics if they don't already exist
if new_topics:
    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"✅ Created topics: {[topic.name for topic in new_topics]}")
    except Exception as e:
        print(f"❌ Error creating topics: {e}")
else:
    print("⚠️ All topics already exist.")

admin_client.close()
