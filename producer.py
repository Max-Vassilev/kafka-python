import json
import time
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    print(f"Message delivery {'failed' if err else 'succeeded'}: {err or msg.topic()}")

try:
    while True:
        producer.produce(
            topic="kafka_connection",
            key="Kafka Hello",
            value=json.dumps({"message": "Hello from Producer!"}),
            callback=delivery_report
        )
        producer.flush()
        time.sleep(2)
except KeyboardInterrupt:
    print("Producer exited.")
