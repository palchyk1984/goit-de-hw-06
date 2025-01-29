from kafka import KafkaConsumer
from kafka_config import kafka_config
import json
import time

# Define the topic for alerts
ALERTS_TOPIC = "alerts_hellcat_topic"

# Create Kafka Consumer
consumer = KafkaConsumer(
    ALERTS_TOPIC,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='alerts_consumer_hellcat_group'
)

def format_timestamp(ts):
    """Formats timestamp into a readable format."""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts / 1000))

print(f"📡 Subscribed to topic: {ALERTS_TOPIC}. Waiting for alerts...")

try:
    for message in consumer:
        alert = message.value
        alert_topic = message.topic
        sensor_id = alert.get("sensor_id", "Unknown")
        timestamp = alert.get("timestamp", 0)
        value = alert.get("value", "N/A")
        message_text = alert.get("message", "No message provided")

        print(f"\n⚠️ ALERT RECEIVED [{alert_topic.upper()}]")
        print(f"   📍 Sensor ID: {sensor_id[:8]}...")
        print(f"   🕒 Timestamp: {format_timestamp(timestamp)}")
        print(f"   📊 Value: {value}")
        print(f"   📝 Message: {message_text}")
        print("=" * 40)

        # Commit message processing
        consumer.commit()

except KeyboardInterrupt:
    print("\n🛑 Monitoring stopped.")
except Exception as e:
    print(f"❌ Error processing alerts: {e}")
finally:
    consumer.close()
    print("🔌 Kafka connection closed.")
