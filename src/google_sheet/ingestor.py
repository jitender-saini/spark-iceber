import hashlib
import re
from collections.abc import Callable
from datetime import UTC, datetime

import polars as pl
from pydantic import AnyUrl, BaseModel

from util.config import UpdateBookmark
from util.google_sheet import GoogleSheet
from util.logging import get_logger
from util.table_copier import TableIngestor

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
    db_uri: AnyUrl
    gs_secret_name: str


class IngestJob:
    def __init__(
        self,
        google_sheet: GoogleSheet,
        config: JobConfig,
        table_ingestor: TableIngestor,
        update_bookmark: UpdateBookmark,
        temp_dir: str,
        custom_processor: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
    ):
        self.google_sheet = google_sheet
        self.utc_now = datetime.now(UTC)
        self.config = config
        self.table_ingestor = table_ingestor
        self.update_bookmark = update_bookmark
        self.temp_dir = temp_dir
        self.custom_processor = custom_processor

    def run(self) -> None:
        df = self.get_worksheet_df()
        log.info(f'Fetched data from Google Sheet, {df.height} rows')
        if df.is_empty():
            log.info('No data to ingest')
            return
        dump_path = f'{self.temp_dir}/{self.config.table_name.replace(".", "_")}.csv'
        # df.write_parquet(f'{TEMP_PATH}/{self.config.table_name.replace(".", "_")}.parquet')
        df.write_csv(dump_path, separator='|', null_value=None)
        log.info('Saved temp files!')
        self._create_temp_table(df)
        log.info(f'Created temporary table {self.table_ingestor.temp_table}')
        self.table_ingestor.execute(dump_path)
        log.info(f'Ingested data for table {self.config.table_name}!')
        self.update_bookmark(self.utc_now)
        log.info('Updated bookmark: %s', self.utc_now)

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

    def _create_temp_table(self, df: pl.DataFrame):
        log.info(f'Creating temporary table {self.table_ingestor.temp_table}')
        df = df.remove()
        with self.table_ingestor.engine.connect() as conn:
            df.write_database(self.table_ingestor.temp_table, connection=conn, if_table_exists='replace')

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
