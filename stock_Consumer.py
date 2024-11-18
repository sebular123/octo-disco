from confluent_kafka import Consumer, KafkaError
import os
import json
import polars as pl
import time
from multiprocessing import Manager

WAIT=60

time.sleep(WAIT+10)
print('Starting consumer...')

# Shared memory setup
manager = Manager()
shared_data = manager.dict()

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

# Create an empty Polars DataFrame with specified columns and types
shared_data["df"] = pl.DataFrame(
    schema={
        "timestamp": pl.String,   # Column for strings
        "price": pl.Float64         # Column for prices
    }
)


def get_dataframe():
    return shared_data["df"]


def add_to_df(row: pl.DataFrame):
    current_timestamp = row.select(pl.first("timestamp")).item()
    if not shared_data["df"].filter(pl.col("timestamp") == current_timestamp).is_empty():
        shared_data["df"] = shared_data["df"].filter(pl.col("timestamp") != current_timestamp)
        return shared_data["df"].vstack(row)
    elif shared_data["df"].height >= 120:
        min_timestamp = shared_data["df"].select(pl.col("timestamp")).min().item()
        shared_data["df"] = shared_data["df"].filter(pl.col("timestamp") != min_timestamp)

    return shared_data["df"].vstack(row)


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

            decoded_message = msg.value().decode('utf-8')

            deserialized_data = json.loads(decoded_message)

            # New row to append
            new_row = pl.DataFrame({
                "timestamp": deserialized_data["timestamp"],
                "price": deserialized_data["price"]
            })

            # Append the new row
            shared_data["df"] = add_to_df(new_row)

            print(shared_data["df"])

        time.sleep(WAIT)

except KeyboardInterrupt:
    print("Consumer interrupted by user")
finally:
    # Close down consumer gracefully
    consumer.close()