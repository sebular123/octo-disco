from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json
from datetime import datetime, timedelta

def process_stream(stream):
    def find_skipped_minutes(data):
        parsed = json.loads(data)
        timestamp = datetime.strptime(parsed["timestamp"], "%Y-%m-%d %H:%M:%S")
        price = parsed["price"]
        return (timestamp, price)

    def check_gaps(data):
        gaps = []
        data = sorted(data, key=lambda x: x[0])
        for i in range(1, len(data)):
            if (data[i][0] - data[i-1][0]) > timedelta(minutes=1):
                gaps.append(data[i-1][0].strftime("%Y-%m-%d %H:%M:%S"))
        return json.dumps({"gaps": gaps})

    timestamps_and_prices = stream.map(find_skipped_minutes)
    skipped_minutes = timestamps_and_prices.key_by(lambda x: 1).reduce(check_gaps)
    return skipped_minutes


env = StreamExecutionEnvironment.get_execution_environment()
kafka_consumer = FlinkKafkaConsumer(
    topics='conn-events',
    deserialization_schema=SimpleStringSchema(),
    properties={'bootstrap.servers': 'kafka:9092'}
)
kafka_producer = FlinkKafkaProducer(
    topic='gaps-events',
    serialization_schema=SimpleStringSchema(),
    producer_config={'bootstrap.servers': 'kafka:9092'}
)

stream = env.add_source(kafka_consumer)
processed_stream = process_stream(stream)
processed_stream.add_sink(kafka_producer)

env.execute("Detect Timestamp Gaps")

## NOT CURRENTLY WORKING