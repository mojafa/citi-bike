from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    print(dataset_url)
    df = pd.read_csv(dataset_url, compression='zip')
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    # df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as csv file"""
    Path(f"data").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{dataset_file}.csv.zip")
    df.to_parquet(path, compression="gzip")
    return path


@task
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("citibike")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def web_to_gcs() -> None:
    """Fetch data from web and write to GCS"""
    for year in range(2021, 2022):
            for month in range(1, 2):
                yearmonth = f"{year}{month:02}"
                dataset_file = f"tripdata_{yearmonth}"
                dataset_url = f"https://s3.amazonaws.com/tripdata/{yearmonth}-citibike-tripdata.csv.zip"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    web_to_gcs()
