import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config

TOPIC_BUILDING = "building_sensors_liudmyla"
TOPIC_TEMP_ALERTS = "temperature_alerts_liudmyla"
TOPIC_HUM_ALERTS = "humidity_alerts_liudmyla"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username= kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

def create_consumer():
    return KafkaConsumer(
        TOPIC_BUILDING,
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        group_id="alerts_processor_group_liudmyla",
    )

def main():
    consumer = create_consumer()
    producer = create_producer()

    print("Alerts processor started. Listening to building_sensors_liudmyla...")

    try:
        for msg in consumer:
            data = msg.value
            sensor_id = data.get("sensor_id")
            temperature = data.get("temperature")
            humidity = data.get("humidity")
            ts = data.get("timestamp", datetime.utcnow().isoformat())
            print("Received:", data)

            # Temperature check
            if temperature is not None and temperature > 40:
                alert = {
                    "sensor_id": sensor_id,
                    "temperature": temperature,
                    "humidity": humidity,
                    "timestamp": ts,
                    "alert_type": "TEMPERATURE",
                    "message": f"Temperature too high: {temperature}°C > 40°C",
                }
                producer.send(TOPIC_TEMP_ALERTS, value=alert)
                producer.flush()
                print("Temperature alert sent:", alert)

            # Humidity check
            if humidity is not None and (humidity > 80 or humidity < 20):
                alert = {
                    "sensor_id": sensor_id,
                    "temperature": temperature,
                    "humidity": humidity,
                    "timestamp": ts,
                    "alert_type": "HUMIDITY",
                    "message": f"Humidity out of range: {humidity} %",
                }
                producer.send(TOPIC_HUM_ALERTS, value=alert)
                producer.flush()
                print("Humidity alert sent:", alert)
    except KeyboardInterrupt:
        print("Stopping alerts processor...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()

