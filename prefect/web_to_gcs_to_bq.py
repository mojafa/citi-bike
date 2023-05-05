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
    # Read the CSV file(s) in the ZIP file
    with zipfile.ZipFile(data, "r") as zip_ref:
        file_list = zip_ref.namelist()
        csv_files = [f for f in file_list if f.endswith('.csv')]
        if len(csv_files) == 0:
            raise ValueError("ZIP file does not contain a CSV file. Expected one CSV file.")
        for csv_file in csv_files:
            with zip_ref.open(csv_file) as f:
                df = pd.read_csv(f, encoding='latin-1', low_memory=False, dtype={
                    "ride_id": "str",
                    "rideable_type": "str",
                    "started_at": "str",
                    "ended_at": "str",
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
                print(df.head(2))
                print(f"columns: {df.dtypes}")
                print(f"rows: {len(df)}")
                # df['start_station_id'] = pd.to_numeric(df['start_station_id'], errors='coerce').fillna(0)
                # df['end_station_id'] = pd.to_numeric(df['end_station_id'], errors='coerce').fillna(0)
                return df

@task(log_prints=True, name="Writing to GCS bucket")
def write_gcs(df, filename, bucket_name):
    """Upload a pandas DataFrame as a parquet file to GCS"""
    gcs_bucket = GcsBucket.load("citibike")
    gcs_bucket.upload_from_dataframe(df=df, to_path=filename, serialization_format='csv',timeout=1000)

@task(log_prints=True, name="Writing to BQ table")
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("citibike")

    df.to_gbq(
        project_id="citi-bike-385512",
        destination_table="citi-bike-385512.citibike_dw.rides",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow
def web_to_gcs_to_bq():
    bucket_name = "citi_bike_datalake_citi-bike-385512"
    # urls = [
    #     "https://s3.amazonaws.com/tripdata/202207-citbike-tripdata.csv.zip",
    # # # "https://s3.amazonaws.com/tripdata/202303-citibike-tripdata.csv.zip",
    # # # # "https://s3.amazonaws.com/tripdata/201402-citibike-tripdata.zip",

    #  ]
    urls = [
        f"https://s3.amazonaws.com/tripdata/{yearmonth}-citibike-tripdata.csv.zip"
        for year in range(2022, 2023)
        for yearmonth in [f"{year}{month:02d}" for month in range(8, 13)] + [f"{year+1}{month:02d}" for month in range(1, 4)]
    ]

    for url in urls:
        data = download_file(url)
        df = read_csv(data)
        filename = f"{url[34:-4]}"
        write_gcs(df, filename, bucket_name)
    # write_bq(df)

if __name__ == "__main__":
    web_to_gcs_to_bq()
