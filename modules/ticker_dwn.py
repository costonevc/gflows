import requests
import base64
import orjson
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime, timedelta
from os import environ, getcwd
from pathlib import Path
from functools import partial
from polygon.rest import RESTClient
import pandas as pd
import plotly.express as px
import polygon
import pytz
import orjson 

# related documentation: https://github.com/polygon-io/client-python
# https://polygon.io/docs/stocks/getting-started


def fetch_aggs_data(ticker, client):

    ny_tz = pytz.timezone('America/New_York')

    now_ny = datetime.now(ny_tz)

    current_utc_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    for days_back in range(4): 
        try_date = now_ny - timedelta(days=days_back)
        date_str = try_date.date().isoformat() 
        print(f"Trying to fetch data for {date_str}")  

        try:
            response = client.get_aggs(ticker, 1, "day", date_str, date_str)
            if response and response[0].close:
                print(f"Data found for {date_str}")
                return response[0].close, current_utc_time
        except Exception as e:
            print(f"Error fetching data for {date_str}: {e}")

    print("Failed to fetch data for the past three days")
    return None  


def fetch_options_data(client, ticker):
    ny_tz = pytz.timezone('America/New_York')
    today_ny = datetime.now(ny_tz).date().isoformat()

    options_chain = []

    try:
        for o in client.list_snapshot_options_chain(
            ticker,
            params={
                "expiration_date.gte": today_ny,  
            },
        ):
            options_chain.append(o)
    except Exception as e:
        print(f"Error fetching options data: {e}")
        return None

    c_data = []
    for c in options_chain:
        c_data.append([
            c.underlying_asset.ticker, 
            c.details.contract_type, 
            c.details.expiration_date, 
            c.details.strike_price, 
            float(c.open_interest),
            c.day.volume, 
            c.day.close, 
            c.implied_volatility, 
            c.greeks.delta, 
            c.greeks.gamma, 
            c.greeks.theta, 
            c.greeks.vega
        ])

    if c_data:
        c_df = pd.DataFrame(c_data, columns=[
            'ticker', 'side', 'strike_date', 'strike_price', 
            'open_interest', 'volume', 'close_price', 
            'implied_volatility', 'delta', 'gamma', 'theta', 'vega'
        ])
        return c_df
    else:
        print("No options data found.")
        return pd.DataFrame()
 
def fulfill_req(ticker, is_json):
    client = RESTClient(api_key="q0TtwNDqD1yz2pnD96HDLOBTMSKVh2Zl")

    latest_close_price, timestamp_ms = fetch_aggs_data(ticker, client)

    c_df = fetch_options_data(client, ticker)

    ticker = ticker.lower()

    d_format = "json" if is_json else "csv"
    filename = (
        Path(f"{getcwd()}/data/json/{ticker}_quotedata.json")
        if is_json
        else Path(f"{getcwd()}/data/csv/{ticker}_quotedata.csv")
    )
    
    # Ensure the directory exists
    filename.parent.mkdir(parents=True, exist_ok=True)
    
    if is_json:
        with open(filename, "w", encoding="utf-8") as f:
            # Prepare data with separate fields for DataFrame and latest_close_price
            full_data = {
                "options_data": c_df.to_dict(orient="records"),
                "latest_close_price": latest_close_price,
                "timestamp": timestamp_ms
            }
            # Serialize the full data as JSON and write it to the file
            json_data = orjson.dumps(full_data, option=orjson.OPT_INDENT_2).decode('utf-8')
            f.write(json_data)
        print("\nData saved to JSON for", ticker)
    else:
        # change this later
        with open(filename, "w", encoding="utf-8") as f:
            c_df.to_csv(f, index=False)
        print("\nData saved to CSV for", ticker)


def dwn_data(select, is_json):
    pool = ThreadPool()
    tickers_pool = (environ.get("TICKERS") or "^SPX,^NDX,^RUT").strip().split(",")
    if select:  # select tickers to download
        tickers_pool = [f"^{t}" if f"^{t}" in tickers_pool else t for t in select]
    tickers_format = [
        f"_{ticker[1:]}" if ticker[0] == "^" else ticker for ticker in tickers_pool
    ]
    session = requests.Session()
    session.headers.update({"Accept": "application/json" if is_json else "text/csv"})
    # fulfill_req_with_args = partial(fulfill_req, is_json=is_json, session=session)
    fulfill_req_with_args = partial(fulfill_req, is_json=is_json)
    pool.map(fulfill_req_with_args, tickers_format)
    pool.close()
    pool.join()
    print(f"\n\ndownload end: {datetime.now()}\n")


if __name__ == "__main__":
    dwn_data(select=None, is_json=True)
