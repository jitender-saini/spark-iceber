from pyspark.sql import Column, SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

CATALOG_NAME = 'iceberg_catalog'


class Iceberg:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark_context = spark.sparkContext

    def save_spark_df(self, df: SparkDataFrame, full_table_name: str, partition_col: Column) -> None:
        if self.table_exists(full_table_name):
            self._overwrite_partition(df, full_table_name)
        else:
            self._create_table(df, full_table_name, partition_col)

    @staticmethod
    def _create_table(df: SparkDataFrame, full_table_name: str, partition_col: Column) -> None:
        (df.writeTo(f'{CATALOG_NAME}.{full_table_name}').tableProperty('format-version', '2').partitionedBy(partition_col).create())

    @staticmethod
    def _overwrite_partition(df: SparkDataFrame, full_table_name: str) -> None:
        df.writeTo(f'{CATALOG_NAME}.{full_table_name}').overwritePartitions()

    def table_exists(self, full_table_name: str) -> bool:
        schema_name, table_name = full_table_name.split('.')
        return (
            self._schema_exists(schema_name)
            and self.spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{schema_name} LIKE '{table_name}'").count() > 0
        )

    def _schema_exists(self, schema_name: str) -> bool:
        return self.spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME} LIKE '{schema_name}'").count() > 0

    def query(self, schema: str, table: str, where_clause: str = '1=1') -> SparkDataFrame:
        return self.spark.sql(f'SELECT * FROM {CATALOG_NAME}.{schema}.{table} WHERE {where_clause}')

    def read_parquet(self, path: str) -> SparkDataFrame:
        return self.spark.read.parquet(path)
