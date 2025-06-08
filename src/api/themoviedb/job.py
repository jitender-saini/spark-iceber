import os
import shutil
from datetime import UTC, datetime
from typing import Callable

import polars as pl
from pydantic import AnyUrl, BaseModel

from api.themoviedb.themoviedb import TMDBApi
from util.config import ConfigFactory, UpdateBookmark
from util.connection_factory import ConnectionFactory
from util.local_env import CONFIG_URI, TEMP_PATH
from util.logging import configure_logging, get_logger
from util.secret_manager import SecretManager
from util.table_copier import PostgresTableIngestor

configure_logging()
log = get_logger(__name__)


class JobConfig(BaseModel):
    table_name: str
    bookmark: datetime
    is_active: bool
    primary_keys: list[str]
    range_column: str
    load_type: str
    db_uri: AnyUrl
    secret_name: str


def transform_movies(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([pl.col('release_date').cast(pl.Date)])
    df = df.with_columns(pl.col('genre_ids').list.eval(pl.element().cast(pl.Utf8)).list.join(','))
    return df


class TMDBJob:
    def __init__(
        self, config: JobConfig, tmdb_api: TMDBApi, table_ingestor: PostgresTableIngestor, temp_dir: str, update_bookmark: UpdateBookmark
    ):
        self.config = config
        self.tmdb_api = tmdb_api
        self.table_ingestor = table_ingestor
        self.utc_now = datetime.now(UTC)
        self.temp_dir = temp_dir
        self.update_bookmark = update_bookmark

    def run(self):
        log.info(f'Starting job for {self.config.table_name}')
        movies_data = self.tmdb_api.fetch_movies(self.config.bookmark, start_page=1, max_pages=10)
        self._save_data(movies_data, transform_movies)

    def _save_data(self, data, custom_processor: Callable[[pl.DataFrame], pl.DataFrame] | None = None):
        df = pl.DataFrame(data)
        log.info(f'Fetched {df.height} movies')
        if custom_processor:
            df = custom_processor(df)
        dump_path = f'{self.temp_dir}/{self.config.table_name.replace(".", "_")}.csv'
        df.write_csv(dump_path, separator='|', null_value=None)
        self._create_temp_table(df)
        self.table_ingestor.execute(dump_path)
        self.update_bookmark(self.utc_now)

    def _create_temp_table(self, df: pl.DataFrame):
        log.info(f'Creating temporary table {self.table_ingestor.temp_table}')
        df = df.remove()
        with self.table_ingestor.engine.connect() as conn:
            df.write_database(self.table_ingestor.temp_table, connection=conn, if_table_exists='replace')


def main(config_uri: str = f'{CONFIG_URI}/tmdb_config.json'):
    config_repo = ConfigFactory.from_uri(config_uri)
    config = config_repo.get(JobConfig)
    secret_manager = SecretManager()
    temp_dir = f'{TEMP_PATH}/themoviedb'
    conn = ConnectionFactory.from_uri(str(config.db_uri))

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    with conn.get_sqlalchemy_engine() as engine:
        table_ingestor = PostgresTableIngestor(
            engine=engine,
            table=config.table_name,
            load_timestamp=datetime.now(UTC),
            primary_keys=config.primary_keys,
            range_column=config.range_column,
        )

        tmdb_api = TMDBApi(secret_manager.get_secret('themoviedb_api').get('token'), temp_dir)

        TMDBJob(
            config=config, tmdb_api=tmdb_api, table_ingestor=table_ingestor, temp_dir=temp_dir, update_bookmark=config_repo.update
        ).run()

    if os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir)


if __name__ == '__main__':
    main()
