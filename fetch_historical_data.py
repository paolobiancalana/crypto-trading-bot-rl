import pandas as pd
import requests
import time
import json
from tqdm import tqdm

def fetch_historical_data(symbol, interval, start_time, end_time, api_key):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time * 1000,  # Binance usa timestamp in millisecondi
        "endTime": end_time * 1000,
        "limit": 1000
    }
    headers = {
        "X-MBX-APIKEY": api_key
    }
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"])
    return df

def fetch_multiple_intervals(symbol, interval, start_time, end_time, api_key, num_requests, sleep_time=60):
    data_frames = []
    interval_duration = 1000 * 60  # 1000 minuti
    for i in tqdm(range(num_requests)):
        start = start_time + i * interval_duration
        end = min(start + interval_duration, end_time)
        df = fetch_historical_data(symbol, interval, start, end, api_key)
        data_frames.append(df)
        if i < num_requests - 1:
            time.sleep(sleep_time)  # Sleep per un minuto tra le chiamate per evitare di sovraccaricare l'API
    return pd.concat(data_frames, ignore_index=True)

with open("config.json", "r") as f:
    config = json.load(f)

api_key = config["binance_api_key"]
symbol = "BTCUSDT"
interval = "1m"
end_time = int(time.time())
start_time = end_time - 5 * 365 * 24 * 60 * 60

num_requests = 1314
sleep_time = 60

print("Start time:", time.strftime("%d/%m/%Y"))
print("Fetching data...")
data = fetch_multiple_intervals(symbol, interval, start_time, end_time, api_key, num_requests, sleep_time)
print("End time:", time.strftime("%d/%m/%Y"))
print("Saving data...")
data.to_parquet("bitcoin_5_years_1m.parquet")
print("Done.")
