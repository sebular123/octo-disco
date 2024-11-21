from confluent_kafka import Consumer, KafkaError
import json
import polars as pl
from multiprocessing import Manager


class KafkaConsumerHandler:
    def __init__(self, topic="conn-events", wait_time=60):
        self.topic = topic
        self.wait_time = wait_time

        # Shared memory setup
        manager = Manager()
        self.shared_data = manager.dict()
        self.shared_data["df"] = pl.DataFrame(
            schema={
                "timestamp": pl.String,
                "price": pl.Float64
            }
        )

        # Kafka configuration
        self.conf = {
            'bootstrap.servers': "kafka:9092",
            'group.id': 'my-group',
            'auto.offset.reset': 'earliest'
        }

    def get_dataframe(self):
        return self.shared_data["df"]

    def add_to_df(self, row: pl.DataFrame):
        current_timestamp = row.select(pl.first("timestamp")).item()
        if not self.shared_data["df"].filter(pl.col("timestamp") == current_timestamp).is_empty():
            self.shared_data["df"] = self.shared_data["df"].filter(pl.col("timestamp") != current_timestamp)
            return self.shared_data["df"].vstack(row)
        elif self.shared_data["df"].height >= 120:
            min_timestamp = self.shared_data["df"].select(pl.col("timestamp")).min().item()
            self.shared_data["df"] = self.shared_data["df"].filter(pl.col("timestamp") != min_timestamp)

        return self.shared_data["df"].vstack(row)

    def start_consumer(self):
        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])
        print("Consumer started and listening...")

        try:
            while True:
                msg = consumer.poll(1.0)  # Wait up to 1 second for a message
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition at offset {msg.offset()}")
                    elif msg.error():
                        print(f"Error: {msg.error()}")
                else:
                    # Decode and process the message
                    decoded_message = msg.value().decode('utf-8')
                    deserialized_data = json.loads(decoded_message)

                    new_row = pl.DataFrame({
                        "timestamp": deserialized_data["timestamp"],
                        "price": deserialized_data["price"]
                    })

                    self.shared_data["df"] = self.add_to_df(new_row)
                print(self.shared_data["df"])

        except KeyboardInterrupt:
            print("Consumer interrupted by user")
        finally:
            consumer.close()
