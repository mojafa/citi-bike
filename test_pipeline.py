import requests
import json
import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from datetime import datetime
from pathlib import Path



@task(log_prints=True)
def fetch_data():
    """Fetch gold data from api into pandas DataFrame"""
    # insert code from above to fetch data and save to parquet file
    # set API key and base URL
    api_key = 'MZEBJG8H3L5VEMBY'
    base_url = 'https://www.alphavantage.co/query'
       # set query parameters
    params = {
        'function': 'FX_DAILY',
        'from_symbol': 'XAU',
        'to_symbol': 'USD',
        'apikey': api_key
    }

    # make API request and parse response as JSON
    r = requests.get(base_url, params=params)
    data = json.loads(response.text)

    # convert JSON response to pandas dataframe
    df = pd.DataFrame.from_dict(data['Time Series FX (Daily)'], orient='index')
    df.index.name = 'date'
    df.reset_index(inplace=True)
    df.rename(columns={'4. close': 'close'}, inplace=True)
    df['symbol'] = 'XAUUSD'

    # save dataframes to parquet
    df.to_parquet('data/gold.parquet')
    return df

@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleaning gold dataframe"""

    # convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])

    # convert close column to float
    df['close'] = df['close'].astype(float)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write cleaned data to local parquet file"""
    Path(f"data").mkdir(parents=True, exist_ok=True)
    df.to_parquet('data/gold.parquet')
    return df


@task(log_prints=True)
def upload_to_gcs(filename):
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("finnhub-gcs")
    gcs_block.upload_from_path(from_path='data/gold_us30.parquet', to_path='data/gold_us30.parquet')
    return


@flow()
def web_to_gcs() -> None:
    year = 2019
    for month in range(1, 13):
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(df_clean, dataset_file)
        write_gcs(path)


if __name__ == "__main__":
    web_to_gcs()
