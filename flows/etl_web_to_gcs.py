from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta

#@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
@task(retries=3)

def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe"""
    # if randint(0,1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    print(df.columns)
    return df

@task(log_prints=True)
def clean(df:pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write a dataframe out locally as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-de-gcs")
    gcs_block.upload_from_path(from_path = f"{path}",to_path = path)
    return 


@flow()
def etl_web_to_gcs(year:int, month:int, color:str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path=path)

@flow()
def etl_parent_flow(months: list[int] = [1,2], year: int = 2021, color:str = "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__=="__main__":
    # color = "yellow"
    # months = [1,2,3]
    # year = 2021
    # etl_parent_flow(months, year, color)
    color = "green"
    months = [1]
    year = 2020
    etl_parent_flow(months, year, color)
