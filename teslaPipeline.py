import requests
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path


@task(log_prints=True)
def fetch_data():
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

    return df


@task(log_prints=True)
def filtered_data(df):
    # Filter DataFrame to March 2023
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df[df['timestamp'].dt.year == 2023]
    df = df[df['timestamp'].dt.month == 3]

    # Save DataFrame as CSV file
    path = Path('data').mkdir(parents=True, exist_ok=True)


    df.to_csv('data/March2023.csv', index=False)

    # Print first few rows of the DataFrame
    print(df.head(2))
    print(f"colums: {df.dtypes}")
    print(f"rows: {len(df)}")


   # save dataframes to parquet
    df.to_parquet('data/March2023.parquet')
    print(df)
    return df

@task(log_prints=True)
def upload_to_gcs(df):
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("finnhub-gcs")
    gcs_block.upload_from_path(from_path='data/march.parquet', to_path='data/march.parquet')
    print('Uploaded to GCS')







# Define the flow
@flow(name="tesla ETL")
def parent_flow():
     # """Main ETL function"""
    data = fetch_data()
    dataset_file = filtered_data(data)
    upload_to_gcs(dataset_file)

parent_flow()
