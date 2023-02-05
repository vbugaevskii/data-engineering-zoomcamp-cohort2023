from datetime import timedelta
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket
from prefect_sqlalchemy import SqlAlchemyConnector
from typing import List


def create_yellow_table():
    return '''
    CREATE TABLE IF NOT EXISTS default.yellow_taxi_data
    (
        VendorID                              Int64,
        tpep_pickup_datetime                   Date,
        tpep_dropoff_datetime                  Date,
        passenger_count                       Int64,
        trip_distance                         Float,
        RatecodeID                            Int64,
        store_and_fwd_flag   LowCardinality(String),
        PULocationID                          Int64,
        DOLocationID                          Int64,
        payment_type                          Int64,
        fare_amount                           Float,
        extra                                 Float,
        mta_tax                               Float,
        tip_amount                            Float,
        tolls_amount                          Float,
        improvement_surcharge                 Float,
        total_amount                          Float,
        congestion_surcharge                  Float
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(tpep_pickup_datetime)
    ORDER BY tpep_pickup_datetime
    SETTINGS index_granularity = 8192
    '''


def create_green_table():
    return '''
    CREATE TABLE IF NOT EXISTS default.green_taxi_data
    (
        VendorID                             Int64,
        lpep_pickup_datetime                  Date,
        lpep_dropoff_datetime                 Date,
        store_and_fwd_flag  LowCardinality(String),
        RatecodeID                           Int64,
        PULocationID                         Int64,
        DOLocationID                         Int64,
        passenger_count                      Int64,
        trip_distance                        Float,
        fare_amount                          Float,
        extra                                Float,
        mta_tax                              Float,
        tip_amount                           Float,
        tolls_amount                         Float,
        ehail_fee                            Float,
        improvement_surcharge                Float,
        total_amount                         Float,
        payment_type                         Int64,
        trip_type                            Int64,
        congestion_surcharge                 Float
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(lpep_pickup_datetime)
    ORDER BY tpep_pickup_datetime
    SETTINGS index_granularity = 8192
    '''


def drop_table(table):
    return f'DROP TABLE IF EXISTS {table}'


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    AwsCredentials.load("yandex-cloud-s3-credentials")

    s3_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.download_object_to_path(from_path=s3_path, to_path=s3_path)
    
    return s3_path


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Data cleaning example"""
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame, color) -> None:
    """Write DataFrame to BiqQuery"""
    with SqlAlchemyConnector.load("yandex-cloud-clickhouse-connector") as con:
        print("Connection:", con)
        print("Engine:", con.get_engine())
        
        # NOTE: We can't insert data into ClickHouse without engine, so we have to create it 
        # https://github.com/xzkostyan/clickhouse-sqlalchemy/issues/20
        if color == 'green':
            sql_query = create_green_table()
        elif color == 'yellow':
            sql_query = create_yellow_table()
        else:
            raise ValueError('Unsupported color', color)
        # print("Create table:\n", sql_query)
        con.execute(sql_query)

        df.to_sql(name=f'{color}_taxi_data', con=con.get_engine(),
                  chunksize=100_000, if_exists='append', index=False)


@flow()
def etl_gcs_to_bq(month: int = 1, year: int = 2020, color: str = "green") -> int:
    """Main ETL flow to load data into Big Query"""
    s3_path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(s3_path)
    # df = transform(df)
    write_bq(df, color)
    return df.shape[0]


@flow(log_prints=True)
def etl_gcs_to_bq_multiple(months: List[int] = [2, 3], year: int = 2019, color: str = "yellow") -> None:
    rows_processed = 0
    for month in months:
        print(f"Processing {year}-{month} partition; color = {color}")
        rows_processed += etl_gcs_to_bq(month=month, year=year, color=color)
        print(f"Total number of rows proccessed: {rows_processed}")


if __name__ == "__main__":
    etl_gcs_to_bq_multiple()
