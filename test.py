
import yfinance as yf
import time
import requests
import json
import os
import datetime
# Kafka producer configuration

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer <token>'
}

# Ticker symbol of the stock (e.g., Apple Inc.)
ticker_symbol = 'BTC-USD'

#Function to fetch stock price and send to Kafka
def fetch_and_send_stock_price():
    while True:
        try:
            url = 'https://query2.finance.yahoo.com/v8/finance/chart/btc-usd'
            print(f"Getting data @ {str(datetime.datetime.now())}")
            response = requests.get(url, headers=headers)
            data = json.loads(response.text)
            
            current_price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            current_timestamp = str(datetime.datetime.fromtimestamp(data["chart"]["result"][0]["meta"]["regularMarketTime"]))

            data_to_send = {"timestamp": current_timestamp, "price": current_price}
            print(data_to_send)

            data_to_send = json.dumps(data_to_send, default=str).encode('utf-8')

            sent = data_to_send.decode('utf-8')
            
            deserialized_data = json.loads(sent)
            print(deserialized_data)

        except Exception as e:
            print(f"Error fetching/sending stock price: {e}")

        # Sleep for a specified interval (e.g., 5 seconds) before fetching the next price
        time.sleep(60)

# Start sending stock price data
fetch_and_send_stock_price()
