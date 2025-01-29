from kafka import KafkaConsumer, KafkaProducer
from kafka_config import kafka_config
import json

# Define the input and output topics
INPUT_TOPIC = "building_sensors_hellcat_topic"
OUTPUT_TOPIC = "alerts_hellcat_topic"

# Configure Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='sensor_processor_hellcat_group'
)

# Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

print("📡 Started processing sensor data...")

# Process incoming messages
for message in consumer:
    data = message.value
    print(f"📥 Received message: {data}")  # Debug message

    # Validate message format
    if not isinstance(data, dict):
        print(f"❌ Invalid message format: {data}")
        continue

    # Check if required keys are present
    if "temperature" not in data or "humidity" not in data:
        print(f"⚠️ Missing expected keys in message: {data}")
        continue

    # Generate alerts based on thresholds
    alerts = []

    if data["temperature"] > 40:
        alerts.append({
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "value": data["temperature"],
            "type": "temperature",
            "message": "🚨 Critical temperature!"
        })

    if data["humidity"] > 80 or data["humidity"] < 20:
        alerts.append({
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "value": data["humidity"],
            "type": "humidity",
            "message": "🚨 Abnormal humidity!"
        })

    # Send alerts to the output topic
    for alert in alerts:
        producer.send(OUTPUT_TOPIC, key=data["sensor_id"], value=alert)
        print(f"⚠️ Alert sent: {alert}")

    # Commit message processing
    consumer.commit()
