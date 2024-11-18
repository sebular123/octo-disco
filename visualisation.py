from stock_Consumer import get_dataframe
import time  # to simulate a real time data, time loop
import polars as pl
import plotly.express as px  # interactive charts
import numpy as np
import streamlit as st  # 🎈 data web app development

st.set_page_config(
    page_title="Real-Time Bitcoin Feed",
    page_icon="✅",
    layout="wide",
)

df = get_dataframe()

# dashboard title
st.title("Real-Time / Live Bitcoin Feed")

# creating a single-element container
placeholder = st.empty()

with placeholder.container():

    st.markdown("### Visualisation")
    st.line_chart(df, x="timestamp", y="price")

    st.markdown("### Detailed Data View")
    st.dataframe(df)
    time.sleep(1)