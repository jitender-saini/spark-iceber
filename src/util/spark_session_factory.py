from urllib.parse import urlparse

from pyspark.sql import SparkSession

from util.local_env import JDBC_CATALOG_DIR


class SparkSessionFactory:
    @classmethod
    def from_uri(cls, catalog_uri: str) -> SparkSession:
        parsed_uri = urlparse(catalog_uri)
        catalog_name = parsed_uri.scheme
        if catalog_name == 'jdbc':
            s3_endpoint = f'http://{parsed_uri.hostname}:{parsed_uri.port}'
            bucket = parsed_uri.path[1:]
            s3_access_key_id = parsed_uri.username
            s3_secret_access_key = parsed_uri.password
            return cls._jdbc(
                s3_endpoint=s3_endpoint,
                s3_access_key_id=s3_access_key_id,
                s3_secret_access_key=s3_secret_access_key,
                bucket=bucket,
            )
        else:
            raise ValueError(f'Unsupported URI scheme: {catalog_name}')

    @staticmethod
    def _jdbc(s3_endpoint: str, s3_access_key_id: str, s3_secret_access_key: str, bucket: str) -> SparkSession:
        return (
            SparkSession.builder.appName(bucket)
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .config('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
            .config('spark.sql.catalog.iceberg_catalog.catalog-impl', 'org.apache.iceberg.jdbc.JdbcCatalog')
            .config('spark.sql.catalog.iceberg_catalog.uri', f'jdbc:duckdb:{JDBC_CATALOG_DIR}/{bucket}.db')
            .config('spark.sql.catalog.iceberg_catalog.warehouse', f's3://{bucket}/')
            .config('spark.sql.catalog.iceberg_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
            .config('spark.sql.catalog.iceberg_catalog.s3.endpoint', s3_endpoint)
            .config('spark.sql.catalog.iceberg_catalog.s3.access-key-id', s3_access_key_id)
            .config('spark.sql.catalog.iceberg_catalog.s3.secret-access-key', s3_secret_access_key)
            .config('spark.sql.iceberg.handle-timestamp-without-timezone', 'true')
            .config('spark.sql.session.timeZone', 'UTC')
            .config('spark.hadoop.fs.s3a.endpoint', s3_endpoint)
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.access.key', s3_access_key_id)
            .config('spark.hadoop.fs.s3a.secret.key', s3_secret_access_key)
            .config('spark.network.crypto.enabled', 'false')
            .config("spark.driver.memory", "8g")  # temporary fix for local testing
            .config("spark.executor.memory", "8g") # temporary fix for local testing
            .getOrCreate()
        )
