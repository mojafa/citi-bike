import requests
import json
import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from datetime import datetime



@task
def fetch_data():
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
    response = requests.get(base_url, params=params)
    data = json.loads(response.text)

    # convert JSON response to pandas dataframe
    df = pd.DataFrame.from_dict(data['Time Series FX (Daily)'], orient='index')
    df.index.name = 'date'
    df.reset_index(inplace=True)
    df.rename(columns={'4. close': 'close'}, inplace=True)
    df['symbol'] = 'XAUUSD'

    # repeat above steps for US30
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': 'DJI',
        'apikey': api_key
    }
    response = requests.get(base_url, params=params)
    data = json.loads(response.text)
    df_us30 = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index')
    df_us30.index.name = 'date'
    df_us30.reset_index(inplace=True)
    df_us30.rename(columns={'4. close': 'close'}, inplace=True)
    df_us30['symbol'] = 'US30'

    # concatenate dataframes
    df_all = pd.concat([df, df_us30])

    # save data to parquet file and upload to GCS
    df_all.to_parquet('<path_to_parquet_file>')


@task
def upload_to_gcs(filename):
    client = storage.GCSClient()
    bucket_name = 'finnhub-gcs'
    client.upload(
        bucket=bucket_name,
        location=f'data/{datetime.today().strftime("%Y-%m-%d")}/{filename}',
        filepath=filename,
        mime_type='application/octet-stream'
    )

    with Flow('fetch_and_upload_data') as flow:
        data = fetch_data()
        filename = 'data.parquet'
        upload_to_gcs(filename)
        flow.run()
