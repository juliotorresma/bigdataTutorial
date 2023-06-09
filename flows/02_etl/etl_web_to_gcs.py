from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read taxi data from web into pandas DataFrame'''

    df = pd.read_csv(dataset_url)

    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    '''Fix dtype issues'''
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    '''Write DataFrame out locally as parquet file'''
    path = Path(f"data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    print("Hizo la escritura en local...")
    return path

@task(retries=3)
def write_gcs(path: Path) -> None:
    '''Upload local parquet file to GCS'''
    gcs_block = GcsBucket.load("sii-bucket")
    gcs_block.upload_from_path(
        from_path = f"{path}",
        to_path= path
    )
    return 

@flow()
def etl_web_to_gcs()->None:
    ''' Main ETL Function ''' 
    color = "green"    
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()