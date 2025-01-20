from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weather_reader',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['kafka_connection'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print("Waiting...")
        elif msg.error():
            print(f"Error: {msg.error()}")
        else:
            print(f"Key: {msg.key().decode()}, Value: {msg.value().decode()}")
finally:
    consumer.close()
