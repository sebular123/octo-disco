from stock_Consumer import get_dataframe
import streamlit as st
import time
import polars as pl
import plotly.express as px
import numpy as np

st.set_page_config(
    page_title="Real-Time Bitcoin Feed",
    page_icon="✅",
    layout="wide",
)

# Dashboard title
st.title("Real-Time / Live Bitcoin Feed")

# Creating a single-element container
placeholder = st.empty()

# Refresh loop
while True:
    # Fetch the latest DataFrame
    df = get_dataframe()

    with placeholder.container():
        st.markdown("### Visualisation")
        if df.height > 0:  # Ensure the DataFrame is not empty
            fig = px.line(df, x="timestamp", y="price", title="Bitcoin Price Over Time")
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Detailed Data View")
        st.dataframe(df)

    time.sleep(10)  # Wait before the next update
