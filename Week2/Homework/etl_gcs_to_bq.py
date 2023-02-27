from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    return df


@task()
def write_bq(df: pd.DataFrame, color:str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcs")

    df.to_gbq(
        destination_table=f"trips_data_all.{color}_tripdata",
        project_id="dataengineeringzoocamp",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(month:int, year:int, color:str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df, color)


@flow()
def etl_parent_flow(
    months: list[int] = [1], years: list[int] = [2020], colors: list[str] = ["green"]
):
    for color in colors:
        for year in years:
            for month in months:
                etl_gcs_to_bq(month, year, color)


if __name__ == "__main__":
    months = [month for month in range(1,13)]
    years = [2019, 2020]
    colors = ["green", "yellow"]
    etl_parent_flow(months, years, colors)
