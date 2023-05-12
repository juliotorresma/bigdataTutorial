from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int)->Path:
    '''Download data from GCS'''
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path = gcs_path)
    return Path(f"{gcs_path}") 

@task()
def transform(path: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_parquet(path)
    print(f"pre: missing passanger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passanger count: {df['passenger_count'].isna().sum()}")

    return df

@task()
def write_bq(df: pd.DataFrame,color) -> None:
    '''Write DataFrame to BigQuery'''
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table=f"trips_data_all.{color}",
        project_id="dtc-de-378723",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    '''Main ETL flow to load adata into Big Query'''
    color = "green"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df, color)

if __name__=="__main__":
    etl_gcs_to_bq()