# 1. Import required libraries
import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime

# 2. Set up directory to save simulated streaming data
output_dir = "global_stock_data"
os.makedirs(output_dir, exist_ok=True)

# 3. Define the stock indices we want
indices = ['^DJI', '^IXIC', '^FTSE', '^N225', '000001.SS']

# 4. Function to fetch and save data
def fetch_and_save():
    for ticker in indices:
        data = yf.download(tickers=ticker, period='1d', interval='1m')
        latest = data.tail(1)  # Get the latest record
        if not latest.empty:
            latest['Ticker'] = ticker
            filename = f"{indices}.csv"
            filepath = os.path.join(output_dir, filename)

            if os.path.exists(filepath):
                # File exists: append without duplicating header
                latest.to_csv(filepath, mode='a', header=False)
            else:
                # File doesn't exist: create new file with header
                latest.to_csv(filepath)

            print(f"Updated {filepath}")

# 5. Polling loop (every 1 min for example)
while(True):  # for example, run 5 times only (you can run forever)
    fetch_and_save()
    time.sleep(60)  # wait 1 minute
