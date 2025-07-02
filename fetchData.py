import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# Binance API endpoint
base_url = 'https://fapi.binance.com/fapi/v1/klines'

# Parameters
symbol = 'ETHUSDT'
interval = '15m'
limit = 1000  # Binance's max per request

# Calculate start time (3 years back)
start_time = int((datetime.now() - timedelta(days=3 * 365)).timestamp() * 1000)

candles = []
print("Fetching data...")

while True:
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit,
        'startTime': start_time
    }

    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Request failed: {e}")
        time.sleep(5)
        continue

    data = response.json()

    if not data:
        print("No more data. Finished fetching.")
        break

    candles.extend(data)

    print(f"Fetched up to: {datetime.fromtimestamp(data[-1][0] / 1000)} | Total candles: {len(candles)}")

    # Prepare for next batch
    start_time = data[-1][0] + 1

    # Respect Binance API rate limits
    time.sleep(0.5)

    # Exit if last candle is within last 15 minutes
    if datetime.fromtimestamp(data[-1][0] / 1000) > datetime.now() - timedelta(minutes=15):
        print("Reached recent data. Stopping fetch.")
        break

# Prepare final DataFrame
df = pd.DataFrame(candles, columns=[
    'timestamp', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_asset_volume', 'number_of_trades',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
])

# Keep only required columns
df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

# Convert timestamp to readable format
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# Save to CSV
df.to_csv('ETHUSDT_15min_cleaned.csv', index=False)
print('Data saved to ETHUSDT_15min_cleaned.csv')
