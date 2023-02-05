import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    color_date_prefix = {
        "green":"lpep",
        "yellow":"tpep"
    }
    pick_up_datetime = f"{color_date_prefix[color]}_pickup_datetime"
    drop_off_datetime = f"{color_date_prefix[color]}_dropoff_datetime"

    df[pick_up_datetime] = pd.to_datetime(df[pick_up_datetime])
    df[drop_off_datetime] = pd.to_datetime(df[drop_off_datetime])
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    dir_path = f"data/{color}/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
       
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year:int, month:int, color:str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] = [1], year: int = 2020, color: str = "green"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    months = [2,3]
    year = 2019
    color = "yellow"
    etl_parent_flow(months, year, color)
