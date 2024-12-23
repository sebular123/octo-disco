import streamlit as st
import time
from stock_Consumer import KafkaConsumerHandler
import threading

# Streamlit page configuration
st.set_page_config(page_title="Real-Time Bitcoin Feed", page_icon="✅", layout="wide")

# Dashboard title
st.title("Real-Time / Live Bitcoin Feed")

# Initialize the Kafka consumer handlers
pricing_consumer = KafkaConsumerHandler(topic="conn-events")
gaps_consumer = KafkaConsumerHandler(topic="gaps-events")

# Start the Kafka consumer in the background
threading.Thread(target=pricing_consumer.start_consumer, daemon=True).start()
threading.Thread(target=gaps_consumer.start_consumer, daemon=True).start()

# Visualization loop
placeholder = st.empty()
while True:
    pricing_df = pricing_consumer.get_dataframe()
    gaps_df = gaps_consumer.get_dataframe()
    if pricing_df.height > 0:  # Ensure the DataFrame has data before plotting
        with placeholder.container():
            st.line_chart(pricing_df, x="timestamp", y="price")
            st.dataframe(gaps_df)
    else:
        st.write("Waiting for data...")
    time.sleep(10)
