import pandas as pd

from pathlib import Path

from prefect import flow, task
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket

from urllib.request import urlretrieve

import shutil

from typing import List


@task()
def prepare_env(workdir: str, rewrite: bool = True) -> Path:
    workdir = Path(workdir)
    if rewrite:
        shutil.rmtree(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


@task(retries=3, log_prints=True)
def fetch(dataset_url: str, dataset_file: Path) -> Path:
    """Read taxi data from web into pandas DataFrame with type casting"""
    print(f"partition_url={dataset_url}")
    print(f"partition={dataset_file}")
    urlretrieve(dataset_url, dataset_file)
    return dataset_file


@task()
def write_remote(path: Path) -> None:
    """Upload local parquet file to Yandex.Cloud"""
    AwsCredentials.load("yandex-cloud-s3-credentials")
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.upload_from_path(from_path=path, to_path=path)


@flow()
def etl_web_to_gcs(month: int = 1, year: int = 2020, color: str = "green") -> None:
    """The main ETL function"""
    dataset_name = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_name}.csv.gz"

    workdir_path = f"data/{color}"
    workdir = prepare_env(workdir_path, rewrite=False)

    dataset_path = workdir / f"{dataset_name}.cvs.gz"
    write_remote(fetch(dataset_url, dataset_path))


@flow()
def etl_web_to_gcs_multiple(months: List[int] = [2, 3], year: int = 2019, color: str = "green") -> None:
    for month in months:
        etl_web_to_gcs(month=month, year=year, color=color)


if __name__ == "__main__":
    # etl_web_to_gcs()
    etl_web_to_gcs_multiple(color="yellow")
