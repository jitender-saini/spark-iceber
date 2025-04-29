import sys
from datetime import datetime

import polars as pl
from pydantic import AnyUrl, BaseModel

from util.config import ConfigFactory
from util.connection_factory import Connection, ConnectionFactory
from util.google_sheet import GoogleSheet, GoogleSheetFactory
from util.local_env import DUCKDB_URI, GOOGLE_SHEET_SECRET_PATH
from util.logging import configure_logging, get_logger, log_execution_time

configure_logging()
log = get_logger(__name__)


class UploadJob:
    def __init__(
        self,
        google_sheet: GoogleSheet,
        connection: Connection,
        sheet_url: str,
        worksheet_name: str,
        schema_name: str,
        table_name: str,
        columns: list[str],
        where_clause: str | None = None,
        limit: int | None = None,
    ):
        self.gs = google_sheet
        self.conn = connection
        self.sheet_url = sheet_url
        self.worksheet_name = worksheet_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns = columns
        self.where_clause = where_clause
        self.limit = limit

    def run(self):
        query = self._compose_query()
        log.info('Updating Google Sheet with query: %s', query)
        df = self._fetch_data(query)
        log.info(f'Fetched data from Redshift, {df.height} rows')
        self.gs.update_worksheet(url=self.sheet_url, worksheet_name=self.worksheet_name, df=df)
        log.info(f'Uploaded data to Google Sheet {self.sheet_url}, worksheet {self.worksheet_name}')

    def _compose_query(self) -> str:
        query = f'select {", ".join(self.columns)} from {self.schema_name}.{self.table_name}'
        if self.where_clause:
            query += f' {self.where_clause}'
        if self.limit:
            query += f' limit {self.limit}'
        return query

    def _fetch_data(self, query: str) -> pl.DataFrame:
        with self.conn.get_sqlalchemy_engine() as engine, engine.connect() as conn:
            return pl.read_database(query=query, connection=conn)


class JobConfig(BaseModel):
    table_name: str
    schema_name: str
    bookmark: datetime
    sheet_url: AnyUrl
    worksheet_name: str
    columns: list[str]
    where_clause: str | None = None
    limit: int | None = 1000


@log_execution_time(log)
def main(config_uri: str) -> None:
    config_repo = ConfigFactory.from_uri(config_uri)
    config = config_repo.get(JobConfig)
    google_sheet = GoogleSheetFactory.from_credential_json(GOOGLE_SHEET_SECRET_PATH)
    connection = ConnectionFactory.from_uri(DUCKDB_URI)

    job = UploadJob(
        google_sheet=google_sheet,
        connection=connection,
        sheet_url=str(config.sheet_url),
        worksheet_name=config.worksheet_name,
        schema_name=config.schema_name,
        table_name=config.table_name,
        columns=config.columns,
        where_clause=config.where_clause,
        limit=config.limit,
    )
    job.run()


if __name__ == '__main__':
    # TODO: Add error alert
    main(sys.argv[1])
