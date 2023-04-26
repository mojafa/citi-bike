import requests
import json
import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from datetime import datetime
from pathlib import Path

import requests
import pandas as pd

# Set API key and base URL
api_key = 'MZEBJG8H3L5VEMBY'
base_url = 'https://www.alphavantage.co/query'

# Set query parameters
params = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'TSLA',
    'interval': '5min',
    'outputsize': 'full',
    'apikey': api_key
}

# Make API request and parse response as JSON
try:
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(data)

except requests.exceptions.RequestException as e:
    print('Error:', e)
    exit(1)

# Convert JSON response to pandas DataFrame

try:
    df = pd.DataFrame.from_dict(data['Time Series (5min)'], orient='index')
    df.index.name = 'timestamp'
    df.reset_index(inplace=True)
    df.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    }, inplace=True)
    df['symbol'] = 'TSLA'
    print(df)

except KeyError as e:
    print('Error:', e)
    exit(1)

# Filter DataFrame to March 2023
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df[df['timestamp'].dt.year == 2023]
df = df[df['timestamp'].dt.month == 3]

# Save DataFrame as CSV file
df.to_csv('TSLA_March_2023.csv', index=False)

# Print first few rows of the DataFrame
print(df.head())


# # @task(log_prints=True)
# def fetch_data():
#     """Fetch gold data from api into pandas DataFrame"""
#     # insert code from above to fetch data and save to parquet file
#     # set API key and base URL
#     api_key = 'MZEBJG8H3L5VEMBY'
#     base_url = 'https://www.alphavantage.co/query'
#        # set query parameters
#     params = {
#         'function': 'FX_DAILY',
#         'from_symbol': 'XAU',
#         'to_symbol': 'USD',
#         'apikey': api_key
#     }

#     # make API request and parse response as JSON
#     r = requests.get(base_url, params=params)
#     data = json.loads(response.text)

#     # convert JSON response to pandas dataframe
#     df = pd.DataFrame.from_dict(data['Time Series FX (Daily)'], orient='index')
#     df.index.name = 'date'
#     df.reset_index(inplace=True)
#     df.rename(columns={'4. close': 'close'}, inplace=True)
#     df['symbol'] = 'XAUUSD'

#     # save dataframes to parquet
#     df.to_parquet('data/gold.parquet')
#     print(df)
#     return df

# # @task(log_prints=True)
# # def clean_data(df: pd.DataFrame) -> pd.DataFrame:
# #     """Cleaning gold dataframe"""
# #     # convert date column to datetime
# #     df['date'] = pd.to_datetime(df['date'])

# #     # convert close column to float
# #     df['close'] = df['close'].astype(float)

# #     print(df.head(2))
# #     print(f"columns: {df.dtypes}")
# #     print(f"rows: {len(df)}")
# #     print(df)

# # @task
# def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
#     """Write cleaned data to local parquet file"""
#     Path(f"data").mkdir(parents=True, exist_ok=True)
#     df.to_parquet('data/gold.parquet')
#     return df


# @task(log_prints=True)
# def upload_to_gcs(filename):
#     """Upload local parquet file to GCS"""
#     gcs_block = GcsBucket.load("finnhub-gcs")
#     gcs_block.upload_from_path(from_path='data/gold_us30.parquet', to_path='data/gold_us30.parquet')
#     return


# @flow()
# def main() -> None:
#     """Main flow"""
#     df = fetch_data()
#     df_clean = clean_data(df)
#     path = write_local(df_clean)
#     upload_to_gcs(path)
