import sys
from datetime import UTC, datetime

import polars as pl
from pydantic import AnyUrl, BaseModel

from util.config import ConfigFactory, UpdateBookmark
from util.connection_factory import Connection, ConnectionFactory
from util.google_sheet import GoogleSheet, GoogleSheetFactory
from util.logging import configure_logging, get_logger, log_execution_time

configure_logging()
log = get_logger(__name__)


class JobConfig(BaseModel):
    table_name: str
    bookmark: datetime
    sheet_url: AnyUrl
    worksheet_name: str
    columns: list[str]
    where_clause: str | None = None
    limit: int | None = 1000
    duckdb_uri: AnyUrl
    gs_secret_path: str


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


@log_execution_time(log)
def main(config_uri: str) -> None:
    config_repo = ConfigFactory.from_uri(config_uri)
    config = config_repo.get(JobConfig)
    google_sheet = GoogleSheetFactory.from_credential_json(config.gs_secret_path)
    connection = ConnectionFactory.from_uri(str(config.duckdb_uri))

    job = UploadJob(
        google_sheet=google_sheet,
        connection=connection,
        config=config,
        update_bookmark=config_repo.update,
    )
    job.run()


if __name__ == '__main__':
    # TODO: Add error alert
    log.info('Starting uploader job, system args: %s', sys.argv)
    main(sys.argv[1])
