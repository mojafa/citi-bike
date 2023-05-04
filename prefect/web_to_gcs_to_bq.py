import pandas as pd
import requests
import io
import zipfile
from google.cloud import storage
import datetime as dt
from pathlib import Path
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, name="Fetch citi bike data", retries=3)
def download_file(url):
    print(f"Downloading data from {url}...")
    response = requests.get(url)
    return io.BytesIO(response.content)


@task(log_prints=True, name="Reading data as dataframe")
def read_csv(data):
    with zipfile.ZipFile(data, "r") as zip_ref:
        file_list = zip_ref.namelist()
        csv_files = [f for f in file_list if f.endswith('.csv')]
        if len(csv_files) == 0:
            raise ValueError("ZIP file does not contain a CSV file. Expected one CSV file.")

        df_list = []
        for csv_file in csv_files:
            with zip_ref.open(csv_file) as f:
                df = pd.read_csv(f, encoding='latin-1')
                df_list.append(df)
        df = max(df_list, key=len)
        df['start_station_id'] = pd.to_numeric(df['start_station_id'], errors='coerce').fillna(0)
        df['end_station_id'] = pd.to_numeric(df['end_station_id'], errors='coerce').fillna(0)
        return df

@task(log_prints=True, name="Writing to GCS bucket")
def write_gcs(df, filename, bucket_name):
    """Upload a pandas DataFrame as a parquet file to GCS"""
    gcs_bucket = GcsBucket.load("citibike")
    gcs_bucket.upload_from_dataframe(df=df, to_path=filename, serialization_format='parquet_snappy',timeout=1000)

@task(log_prints=True, name="Extracting from GCS bucket")
def extract_from_gcs() -> pd.DataFrame:
    """Download and concatenate trip data from GCS"""
    gcs_path = "citi_bike_datalake_citi-bike-385512/"
    gcs_block = GcsBucket.load("citibike")
    blobs=gcs_block.list_blobs("citi_bike_datalake_citi-bike-385512/")
    df_list = []
    # for blob in blobs:
    #     if blob.name.endswith(".parquet"):
    #         df = pd.read_parquet(f"gs://{blob.bucket.name}/{blob.name}")
    #         df_list.append(df)
    # return pd.concat(df_list)
    for blob in blobs:
        if blob.name.endswith(".parquet.snappy"):
            df = pd.read_parquet(f"gcs://{blob.bucket.name}/{blob.name}")
            df_list.append(df)
    return pd.concat(df_list)

@task(log_prints=True, name="Transformaing Data")
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Data cleaning and transformation"""
    df = df.astype({
        "ride_id": "str",
        "rideable_type": "str",
        "started_at": "datetime64[ns]",
        "ended_at": "datetime64[ns]",
        "start_station_name": "str",
        "start_station_id": "str",
        "end_station_name": "str",
        "end_station_id": "str",
        "start_lat": "float64",
        "start_lng": "float64",
        "end_lat": "float64",
        "end_lng": "float64",
        "member_casual": "str"
    })
    return df

@task(log_prints=True, name="Writing to BQ table")
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("citibike")

    df.to_gbq(
        destination_table="citi-bike-385512.citi_bike_rawdata",
        project_id="citi-bike-385512",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow
def web_to_gcs_to_bq():
    bucket_name = "citi_bike_datalake_citi-bike-385512"

    urls = [
        f"https://s3.amazonaws.com/tripdata/{yearmonth}-citibike-tripdata.csv.zip"
        for year in range(2021, 2022)
        for yearmonth in [f"{year}{month:02d}" for month in range(4, 5)] + [f"{year+1}{month:02d}" for month in range(1, 2)]
    ]

    for url in urls:
        data = download_file(url)
        df = read_csv(data)
        df['start_station_id'] = pd.to_numeric(df['start_station_id'], errors='coerce').fillna(0)
        df['end_station_id'] = pd.to_numeric(df['end_station_id'], errors='coerce').fillna(0)

        filename = f"{url.split('/')[-1].replace('.zip', '.parquet')}"
        write_gcs(df, filename, bucket_name)

    df = extract_from_gcs()
    df = transform(df)
    write_bq(df)

if __name__ == "__main__":
    web_to_gcs_to_bq()
