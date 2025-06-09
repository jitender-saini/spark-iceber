from datetime import UTC, datetime

import polars as pl
from pydantic import AnyUrl, BaseModel

from util.config import UpdateBookmark
from util.connection_factory import Connection
from util.google_sheet import GoogleSheet
from util.logging import get_logger

log = get_logger(__name__)


class JobConfig(BaseModel):
    table_name: str
    bookmark: datetime
    sheet_url: AnyUrl
    worksheet_name: str
    columns: list[str]
    where_clause: str | None = None
    limit: int | None = 1000
    db_uri: AnyUrl
    gs_secret_name: str


class UploadJob:
    def __init__(
        self,
        google_sheet: GoogleSheet,
        connection: Connection,
        config: JobConfig,
        update_bookmark: UpdateBookmark,
    ):
        self.gs = google_sheet
        self.conn = connection
        self.config = config
        self.utc_now = datetime.now(UTC)
        self.update_bookmark = update_bookmark

    def run(self):
        query = self._compose_query()
        log.info('Updating Google Sheet with query: %s', query)
        df = self._fetch_data(query)
        df = df.with_columns(pl.lit(self.utc_now).alias('last_updated'))
        log.info(f'Fetched data from Redshift, {df.height} rows')
        self.gs.update_worksheet(url=str(self.config.sheet_url), worksheet_name=self.config.worksheet_name, df=df)
        log.info(f'Uploaded data to Google Sheet {self.config.sheet_url}, worksheet {self.config.worksheet_name}')
        self.update_bookmark(self.utc_now)
        log.info('Updated bookmark: %s', self.utc_now)

    def _compose_query(self) -> str:
        query = f'select {", ".join(self.config.columns)} from {self.config.table_name}'
        if self.config.where_clause:
            query += f' {self.config.where_clause}'
        if self.config.limit:
            query += f' limit {self.config.limit}'
        return query

    def _fetch_data(self, query: str) -> pl.DataFrame:
        with self.conn.get_sqlalchemy_engine() as engine, engine.connect() as conn:
            return pl.read_database(query=query, connection=conn)
