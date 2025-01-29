from kafka import KafkaProducer
from kafka_config import kafka_config
import json
import uuid
import time
import random

# Define the sensor topic
TOPIC_NAME = "building_sensors_hellcat_topic"

# Generate a unique sensor ID
SENSOR_ID = str(uuid.uuid4())

# Configure the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

try:
    print(f"✅ Connected to Kafka. Sending data to topic '{TOPIC_NAME}'...")
    while True:
        # Generate random sensor data
        data = {
            "sensor_id": SENSOR_ID,
            "timestamp": int(time.time() * 1000),
            "temperature": random.randint(20, 50),
            "humidity": random.randint(10, 90)
        }

        # Send the data to Kafka
        producer.send(TOPIC_NAME, key=SENSOR_ID, value=data)
        print(f"📤 Sent: {data}")

        # Wait before sending the next data point
        time.sleep(2)
except KeyboardInterrupt:
    print("\n🛑 Data generation stopped by user.")
finally:
    producer.close()
    print("🔌 Kafka Producer connection closed.")
