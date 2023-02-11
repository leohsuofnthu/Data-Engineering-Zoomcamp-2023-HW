import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task()
def write_local(dataset_url:str, dataset_file: str) -> Path:
    """Download the .GZ file to local"""
    dir_path = f"fhv"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    os.system(f'wget {dataset_url} -P {dir_path}')
    path = Path(f"fhv/{dataset_file}")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    print(f"{path} uploaded successfully.")
    return

@flow()
def etl_web_to_gcs(year:int, month:int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
    path = write_local(dataset_url, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] = [1], year: int = 2019
):
    for month in months:
        etl_web_to_gcs(year, month)
    os.system('rm -rf fhv')

if __name__ == "__main__":
    months = [i for i in range(1,13)]
    year = 2019
    etl_parent_flow(months, year)