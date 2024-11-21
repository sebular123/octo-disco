from confluent_kafka import Producer
import yfinance as yf
import time
import requests
import json
import polars as pl
import os
import random
import datetime
# Kafka producer configuration

WAIT = 60

print('Starting producer...')


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer <token>'
}

conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'stock-price-producer'
}

# Create a Kafka producer instance
producer = Producer(conf)

# Kafka topic to send stock price data
topic = 'conn-events'

# Ticker symbol of the stock (e.g., Apple Inc.)
ticker_symbol = 'BTC-USD'


def get_data_from_URL():
    url = 'https://query2.finance.yahoo.com/v8/finance/chart/btc-usd'
    response = requests.get(url, headers=headers)
    return json.loads(response.text)


#Function to fetch stock price and send to Kafka
def fetch_and_send_stock_price():
    while True:
        try:
            data = get_data_from_URL()

            current_price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            current_timestamp = str(datetime.datetime.fromtimestamp(data["chart"]["result"][0]["meta"]["regularMarketTime"]))

            data_to_send = {"timestamp": current_timestamp, "price": current_price}
            data_to_send = json.dumps(data_to_send, default=str).encode('utf-8')

            # Produce the stock price to the Kafka topic
            producer.produce(topic, key=ticker_symbol, value=data_to_send)
            producer.flush()

            print(f"Sent {ticker_symbol} price to Kafka: {current_price} @ {current_timestamp}")

        except Exception as e:
            print(f"Error fetching/sending stock price: {e}")

        # Sleep for a specified interval (e.g., 5 seconds) before fetching the next price
        time.sleep(WAIT)


# Start sending stock price data
fetch_and_send_stock_price()
