import json
from kafka import KafkaConsumer
from configs import kafka_config

TOPIC_TEMP_ALERTS = "temperature_alerts_liudmyla"
TOPIC_HUM_ALERTS = "humidity_alerts_liudmyla"

def main():
    consumer = KafkaConsumer(
        TOPIC_TEMP_ALERTS,
        TOPIC_HUM_ALERTS,
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="alerts_final_consumer_liudmyla",
    )
    print("Alerts final consumer started. Listening to temperature & humidity alerts...")

    try:
        for msg in consumer:
            topic = msg.topic
            data = msg.value

            print(f"\n---ALERT from topic: {topic} ---")
            print(f"Sensor ID: {data.get('sensor_id')}")
            print(f"Time: {data.get('timestamp')}")
            print(f"Temperature: {data.get('temperature')} °C")
            print(f"Humidity: {data.get('humidity')} %")
            print(f"Type: {data.get('alert_type')}")
            print(f"Message: {data.get('message')}")

            print("-----------------------------------")

    except KeyboardInterrupt:
        print("Stopping alerts final consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()