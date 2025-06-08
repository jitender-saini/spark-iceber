import os

import pytest
from pyspark.sql import DataFrame, SparkSession

from util.local_env import MINIO_ACCESS_KEY_ID, MINIO_HOST, MINIO_PORT, MINIO_SECRET_ACCESS_KEY
from util.logging import get_logger
from util.spark_session_factory import JDBC_CATALOG_DIR, SparkSessionFactory

log = get_logger(__name__)


@pytest.fixture
def spark_uri(minio_bucket) -> str:
    return f'jdbc://{MINIO_ACCESS_KEY_ID}:{MINIO_SECRET_ACCESS_KEY}@{MINIO_HOST}:{MINIO_PORT}/{minio_bucket}'


@pytest.fixture
def spark(minio_bucket: str, spark_uri: str) -> SparkSession:
    _remove_duckdb_files(minio_bucket)
    log.info(f'Creating Spark session with S3 bucket: {minio_bucket}')
    spark = SparkSessionFactory.from_uri(spark_uri)

    yield spark

    spark.stop()
    _remove_duckdb_files(minio_bucket)


def _remove_duckdb_files(db_name):
    for file in os.listdir(JDBC_CATALOG_DIR):
        if file.startswith(db_name):
            os.remove(f'{JDBC_CATALOG_DIR}/{file}')


def extract_column(df: DataFrame) -> list:
    return df.rdd.flatMap(lambda x: x).collect()
