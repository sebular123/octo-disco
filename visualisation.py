import streamlit as st
import time
from stock_Consumer import KafkaConsumerHandler

# Streamlit page configuration
st.set_page_config(page_title="Real-Time Bitcoin Feed", page_icon="✅", layout="wide")

# Dashboard title
st.title("Real-Time / Live Bitcoin Feed")

# Initialize the Kafka consumer handler
kafka_handler = KafkaConsumerHandler()

# Start the Kafka consumer in the background
st.write("Starting Kafka Consumer...")
import threading
threading.Thread(target=kafka_handler.start_consumer, daemon=True).start()

# Visualization loop
placeholder = st.empty()
while True:
    df = kafka_handler.get_dataframe()
    if df.height > 0:  # Ensure the DataFrame has data before plotting
        with placeholder.container():
            st.line_chart(df, x="timestamp", y="price")
            st.dataframe(df)
    else:
        st.write("Waiting for data...")
    time.sleep(10)
