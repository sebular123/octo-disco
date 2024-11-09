from confluent_kafka import Consumer, KafkaError
import os
import time

WAIT=40

time.sleep(WAIT*2)
print('Starting consumer...')


# Get Kafka broker from environment variable
conf = {
    'bootstrap.servers': "kafka:9092",
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to a Kafka topic
topic = 'conn-events'  # Replace with your topic name
consumer.subscribe([topic])

# Consume messages
try:
    while True:
        msg = consumer.poll(1.0)  # Wait up to 1 second for a message
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition at offset {msg.offset()}")
            elif msg.error():
                print(f"Error: {msg.error()}")
        else:
            # Proper message
            print(f"Received message: {msg.value().decode('utf-8')} from {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

        time.sleep(WAIT)
except KeyboardInterrupt:
    print("Consumer interrupted by user")
finally:
    # Close down consumer gracefully
    consumer.close()