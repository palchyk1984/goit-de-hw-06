from kafka import KafkaConsumer
from kafka_config import kafka_config
import json
import sys
import time

TOPIC = "alerts_hellcat_topic"

def create_consumer(topic, group_id='alerts_consumer_group'):
    """
    Creates a Kafka Consumer for the specified topic.
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=group_id
        )
        return consumer
    except Exception as e:
        print(f"❌ Error creating Kafka Consumer: {e}")
        sys.exit(1)

def format_timestamp(ts):
    """Formats a timestamp into a readable format."""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts / 1000))

def main():
    consumer = create_consumer(TOPIC)

    print(f"📡 Listening to alerts in topic: {TOPIC}...")

    try:
        for message in consumer:
            try:
                alert = json.loads(message.value.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"❌ Error decoding JSON: {e} | Raw message: {message.value}")
                continue

            # Determine alert type (single sensor alert or aggregated alert)
            if "avg_temperature" in alert or "avg_humidity" in alert:
                print(f"\n📊 SLIDING WINDOW ALERT [{TOPIC.upper()}]")
                print(f"   🕒 Timestamp: {format_timestamp(alert.get('timestamp', 0))}")
                print(f"   📊 Avg Temperature: {alert.get('avg_temperature', 'N/A')}°C")
                print(f"   💧 Avg Humidity: {alert.get('avg_humidity', 'N/A')}%")
                print(f"   📝 Message: {alert.get('message', 'No message')}")
                print("=" * 40)

            else:
                sensor_id = alert.get("sensor_id", "Unknown")
                timestamp = alert.get("timestamp", 0)
                value = alert.get("value", "N/A")
                message_text = alert.get("message", "No message")

                print(f"\n⚠️ SENSOR ALERT [{TOPIC.upper()}]")
                print(f"   📍 Sensor ID: {sensor_id[:8]}...")
                print(f"   🕒 Timestamp: {format_timestamp(timestamp)}")
                print(f"   📊 Value: {value}")
                print(f"   📝 Message: {message_text}")
                print("=" * 40)

            consumer.commit()
            
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped.")
    except Exception as e:
        print(f"❌ Error processing alerts: {e}")
    finally:
        consumer.close()
        print("🔌 Kafka connection closed.")

if __name__ == "__main__":
    main()
