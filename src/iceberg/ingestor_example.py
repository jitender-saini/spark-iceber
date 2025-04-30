import sys
from datetime import datetime
from typing import Callable

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from util.config import ConfigFactory, UpdateBookmark
from util.iceberg import Iceberg
from util.local_env import CATALOG_URI
from util.logging import configure_logging, get_logger, log_execution_time
from util.spark_session_factory import SparkSessionFactory

configure_logging()
log = get_logger(__name__)


class JobConfig(BaseModel):
    table_name: str
    partition_column: str
    raw_files_path: str
    bookmark: datetime
    is_active: bool


class SampleJob:
    def __init__(
        self,
        config: JobConfig,
        iceberg: Iceberg,
        update_bookmark: UpdateBookmark,
        custom_processor: Callable[[DataFrame], DataFrame] | None = None,
    ):
        self.config = config
        self.iceberg = iceberg
        self.update_bookmark = update_bookmark
        self.custom_processor = custom_processor

    def run(self) -> None:
        df = self.iceberg.read_parquet(self.config.raw_files_path)
        log.info(f'Loaded data, {df.count()} rows')
        if df.isEmpty():
            log.info('No data to ingest')
            return
        if self.custom_processor:
            df = self.custom_processor(df)
        self.iceberg.save_spark_df(df, self.config.table_name, col(self.config.partition_column))


@log_execution_time(log)
def main(config_uri: str) -> None:
    config_repo = ConfigFactory.from_uri(config_uri)
    config = config_repo.get(JobConfig)
    spark = SparkSessionFactory.from_uri(CATALOG_URI)
    iceberg = Iceberg(spark)

    SampleJob(
        config=config,
        iceberg=iceberg,
        update_bookmark=config_repo.update,
    ).run()


if __name__ == '__main__':
    # TODO: Add error alert
    main(sys.argv[1])
