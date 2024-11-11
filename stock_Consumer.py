from confluent_kafka import Consumer, KafkaError
import os
import json
import polars as pl
import time

WAIT=60

time.sleep(WAIT+10)
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

# Create an empty Polars DataFrame with specified columns and types
df = pl.DataFrame(
    schema={
        "timestamp": pl.String,   # Column for strings
        "price": pl.Float64         # Column for prices
    }
)


def add_to_df(df: pl.DataFrame, row: pl.DataFrame):
    current_timestamp = row.select(pl.first("timestamp")).item()
    if not df.filter(pl.col("timestamp") == current_timestamp).is_empty():
        df = df.filter(pl.col("timestamp") != current_timestamp)
        return df.vstack(new_row)
    elif df.height >= 120:
        df = df.filter(pl.col("timestamp") != df["timestamp"].min())

    return df.vstack(new_row)


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
            df = add_to_df(df, new_row)

            print(df)

        time.sleep(WAIT)

except KeyboardInterrupt:
    print("Consumer interrupted by user")
finally:
    # Close down consumer gracefully
    consumer.close()