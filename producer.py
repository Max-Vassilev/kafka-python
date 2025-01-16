import json
from quixstreams import Application
import time


def main():
    app = Application(broker_address="localhost:9092", loglevel="DEBUG")

    with app.get_producer() as producer:
        while True:
            message = {"message": "Hello from Producer!"}

            producer.produce(
                topic="kafka_connection",
                key="Kafka Hello",
                value=json.dumps(message),
            )
            
            time.sleep(2)

if __name__ == "__main__":
    main()

