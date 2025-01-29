import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka_config import kafka_config
import json
import time

# Kafka topics
INPUT_TOPIC = "building_sensors_hellcat_topic"
OUTPUT_TOPIC = "alerts_hellcat_topic"

# Read alert conditions from CSV
try:
    df_alerts = pd.read_csv("alerts_conditions.csv")
    
    # Rename columns to match how they are used in the code
    df_alerts.rename(columns={
        "temperature_min": "min_temp",
        "temperature_max": "max_temp",
        "humidity_min": "min_humidity",
        "humidity_max": "max_humidity"
    }, inplace=True)

    print("✅ Alert conditions loaded successfully:")
    print(df_alerts.head())
except Exception as e:
    print(f"❌ Error loading alert conditions: {e}")
    exit(1)

# Kafka Consumer
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
    group_id='sensor_aggregation_group'
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# Store received data for sliding window aggregation
window_size = 60  # 1-minute window
slide_interval = 30  # Slide every 30 seconds
data_buffer = []

print("📡 Sliding Window Processor started...")

try:
    for message in consumer:
        data = message.value
        print(f"📥 Received data: {data}")

        # Store new data in the buffer
        data_buffer.append(data)

        # Remove old data that is out of the window
        current_time = int(time.time() * 1000)
        data_buffer = [d for d in data_buffer if current_time - d["timestamp"] <= window_size * 1000]

        if len(data_buffer) == 0:
            continue

        # Calculate average temperature and humidity
        avg_temp = sum(d["temperature"] for d in data_buffer) / len(data_buffer)
        avg_humidity = sum(d["humidity"] for d in data_buffer) / len(data_buffer)

        print(f"📊 Sliding Window Aggregation: Avg Temp={avg_temp:.2f}, Avg Humidity={avg_humidity:.2f}")

        # Check for alerts based on conditions
        alerts = []
        for _, condition in df_alerts.iterrows():
            if (
                (condition["min_temp"] == -999 or avg_temp >= condition["min_temp"]) and
                (condition["max_temp"] == -999 or avg_temp <= condition["max_temp"]) and
                (condition["min_humidity"] == -999 or avg_humidity >= condition["min_humidity"]) and
                (condition["max_humidity"] == -999 or avg_humidity <= condition["max_humidity"])
            ):
                alert = {
                    "sensor_id": data["sensor_id"],
                    "timestamp": current_time,
                    "avg_temperature": avg_temp,
                    "avg_humidity": avg_humidity,
                    "alert_code": condition["code"],
                    "message": condition["message"]
                }
                alerts.append(alert)

        # Send alerts to Kafka
        for alert in alerts:
            producer.send(OUTPUT_TOPIC, key=alert["sensor_id"], value=alert)
            print(f"⚠️ Alert Sent: {alert}")

        # Commit Kafka message offset
        consumer.commit()

except KeyboardInterrupt:
    print("\n🛑 Processing stopped.")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    consumer.close()
    producer.close()
    print("🔌 Kafka connection closed.")
