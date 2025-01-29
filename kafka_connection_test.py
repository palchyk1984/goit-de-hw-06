from kafka import KafkaProducer
from kafka_config import kafka_config
import json

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
    producer.send("test-topic", key="test", value={"message": "Hello Kafka!"})
    producer.flush()
    print("✅ Kafka працює!")
except Exception as e:
    print(f"❌ Помилка підключення: {e}")
