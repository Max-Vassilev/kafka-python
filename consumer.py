from quixstreams import Application


app = Application(broker_address="localhost:9092", loglevel="DEBUG", consumer_group="weather_reader")

with app.get_consumer() as consumer:
    consumer.subscribe(["kafka_connection"])

    while True:
        msg = consumer.poll(1)
        
        if msg is None:
            print("Waiting...")
        else:
            key = msg.key().decode('utf8')
            print(msg.value())
