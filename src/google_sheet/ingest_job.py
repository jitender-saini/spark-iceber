import os
import shutil
import sys
from datetime import UTC, datetime

import polars as pl

from google_sheet.ingestor import IngestJob, JobConfig
from util.config import ConfigFactory
from util.connection_factory import ConnectionFactory
from util.google_sheet import GoogleSheetFactory
from util.local_env import TEMP_PATH
from util.logging import configure_logging, get_logger, log_execution_time
from util.secret_manager import SecretManager
from util.table_copier import TableIngestorFactory

configure_logging()
log = get_logger(__name__)


def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col('average_cost').str.replace_all('₹', '').cast(pl.Int32, strict=False),
        pl.col('minimum_order').str.replace_all('₹', '').cast(pl.Int32, strict=False),
        pl.col('rating').cast(pl.Float64, strict=False),
        pl.col('votes').cast(pl.Float64, strict=False),
        pl.col('reviews').cast(pl.Float64, strict=False),
    )


@log_execution_time(log)
def main(config_uri: str) -> None:
    config_repo = ConfigFactory.from_uri(config_uri)
    config = config_repo.get(JobConfig)
    secret = SecretManager().get_secret(config.gs_secret_name)
    google_sheet = GoogleSheetFactory.from_credential_json(secret)
    conn = ConnectionFactory.from_uri(str(config.db_uri))
    temp_dir = f'{TEMP_PATH}/google_sheet'
    log.info('Job args: %s', config)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    if not config.is_active:
        log.info('Job is not active, skipping')
        return
    custom_processor = None
    if config.table_name == 'etl.restaurant':
        custom_processor = transform_data
    with conn.get_sqlalchemy_engine() as engine:
        table_ingestor = TableIngestorFactory.from_connection_type(
            conn_type=conn.type,
            engine=engine,
            table=config.table_name,
            load_timestamp=datetime.now(UTC),
            primary_keys=config.primary_keys,
            range_column=config.range_column,
        )
        job = IngestJob(
            google_sheet=google_sheet,
            config=config,
            table_ingestor=table_ingestor,
            update_bookmark=config_repo.update,
            temp_dir=temp_dir,
            custom_processor=custom_processor,
        )
        job.run()

    if os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir)


if __name__ == '__main__':
    # TODO: Add error alert
    main(sys.argv[1])
