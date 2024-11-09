from confluent_kafka.admin import AdminClient, NewTopic
import os
import time

WAIT=40

time.sleep(WAIT)
print('Starting topic creation...')

# Kafka topic name and configuration
topic_name = "conn-events"
bootstrap_servers = "kafka:9092"

# Initialize AdminClient
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

def create_topic():
    # Check if the topic already exists
    existing_topics = admin_client.list_topics(timeout=10).topics
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists.")
        return

    # Define the new topic
    new_topic = [NewTopic(topic_name, num_partitions=3, replication_factor=1)]

    # Create the topic
    print("Creating topic...")
    fs = admin_client.create_topics(new_topic)

    for topic, future in fs.items():
        try:
            future.result()  # Wait for topic creation to finish
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

# Run the topic creation function
create_topic()
