import hashlib
import re
import sys
from collections.abc import Callable
from datetime import UTC, datetime

import polars as pl
from pydantic import AnyUrl, BaseModel

from util.config import ConfigFactory
from util.connection_factory import ConnectionFactory
from util.google_sheet import GoogleSheet, GoogleSheetFactory
from util.local_env import TEMP_PATH
from util.logging import configure_logging, get_logger, log_execution_time
from util.table_copier import DuckDBTableIngestor

configure_logging()
log = get_logger(__name__)


class JobConfig(BaseModel):
    table_name: str
    bookmark: datetime
    is_active: bool
    sheet_url: AnyUrl
    worksheet_name: str
    primary_keys: list[str]
    range_column: str
    generate_id: bool = False
    sheet_data_row_num: int = 1
    sheet_header_row_num: int = 0
    duckdb_uri: AnyUrl
    gs_secret_path: str


class IngestJob:
    def __init__(
        self,
        google_sheet: GoogleSheet,
        config: JobConfig,
        table_ingestor: DuckDBTableIngestor,
        custom_processor: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
    ):
        self.google_sheet = google_sheet
        self.utc_now = datetime.now(UTC)
        self.config = config
        self.table_ingestor = table_ingestor
        self.custom_processor = custom_processor

    def run(self) -> None:
        df = self.get_worksheet_df()
        log.info(f'Fetched data from Google Sheet, {df.height} rows')
        if df.is_empty():
            log.info('No data to ingest')
            return
        df.write_parquet(f'{TEMP_PATH}/{self.config.table_name.replace(".", "_")}.parquet')
        log.info('Saved temp files!')
        self.table_ingestor.execute(TEMP_PATH)
        log.info(f'Ingested data for table {self.config.table_name}!')

    def get_worksheet_df(self) -> pl.DataFrame:
        data = self.google_sheet.get_worksheet(str(self.config.sheet_url), self.config.worksheet_name)
        df = pl.DataFrame(data[self.config.sheet_data_row_num :], schema=data[self.config.sheet_header_row_num], orient='row')
        if self.config.generate_id:
            df = df.with_columns(pl.struct(df.columns).map_elements(self._generate_unique_id).alias('id'))
        df = df.rename(mapping=self._rename_cols(df.columns))
        if self.custom_processor:
            df = self.custom_processor(df)
        if len(df.columns) != len(set(df.columns)):
            duplicates = [col for col in df.columns if df.columns.count(col) > 1]
            raise ValueError(f'Found duplicate columns: {duplicates}')
        return df

    @staticmethod
    def _rename_cols(cols: list[str]) -> dict[str, str]:
        def to_snake_case(name: str) -> str:
            name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)  # Insert underscore before each uppercase letter that starts a new word
            name = re.sub('([a-z])([A-Z])', r'\1_\2', name)  # Insert underscore before each uppercase letter preceded by a lowercase letter
            name = name.lower()
            name = re.sub(r'[ -]', '_', name)  # Replace spaces and hyphens with underscores
            name = re.sub(r'[^a-zA-Z0-9_]', '', name)  # Remove any characters that are not alphanumeric or underscores
            name = re.sub(r'_{2,}', '_', name)  # Replace multiple consecutive underscores with a single underscore
            name = re.sub(r'^_|_$', '', name)  # Remove leading or trailing underscores
            return name

        return {c: to_snake_case(c) for c in cols}

    @staticmethod
    def _generate_unique_id(row: tuple) -> str:
        id_str = '_'.join(str(i) for i in row)
        return hashlib.md5(id_str.encode()).hexdigest()


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
    google_sheet = GoogleSheetFactory.from_credential_json(config.gs_secret_path)
    conn = ConnectionFactory.from_uri(str(config.duckdb_uri))
    log.info('Job args: %s', config)
    if not config.is_active:
        log.info('Job is not active, skipping')
        return
    custom_processor = None
    if config.table_name == 'elt.restaurant':
        custom_processor = transform_data
    with conn.get_sqlalchemy_engine() as engine:
        table_ingestor = DuckDBTableIngestor(
            engine=engine,
            table=config.table_name,
            load_timestamp=datetime.now(UTC),
            primary_keys=config.primary_keys,
            range_column=config.range_column,
        )
        job = IngestJob(google_sheet=google_sheet, config=config, table_ingestor=table_ingestor, custom_processor=custom_processor)
        job.run()


if __name__ == '__main__':
    # TODO: Add error alert
    main(sys.argv[1])
