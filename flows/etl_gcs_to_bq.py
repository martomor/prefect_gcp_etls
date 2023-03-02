from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Dowload trip data from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load("zoom-de-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f'../data/{gcs_path}')

@task()
def write_to_bg(path: Path) -> None:
    """Write Dataframe to BigQuery"""

    df = pd.read_parquet(path)
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-376701",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@task()
def transform(path: Path) -> pd.DataFrame:
    """Datac cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: mising passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: mising passenger count: {df['passenger_count'].isna().sum()}")
    return df


@flow(log_prints=True)
def elt_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    # color = "yellow"
    # year = 2021
    # month = 1
    color = "green"
    month = 1
    year = 2020

    path = extract_from_gcs(color, year, month)
    write_to_bg(path)


if __name__ == "__main__":
    elt_gcs_to_bq()
