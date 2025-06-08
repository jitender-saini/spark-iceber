import os
import shutil
from datetime import UTC, datetime

import polars as pl
from pydantic import AnyUrl, BaseModel

from api.themoviedb.themoviedb import TMDB
from util.config import ConfigFactory
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


class TMDBJob:
    def __init__(self, config: JobConfig, tmdb: TMDB, table_ingestor: PostgresTableIngestor, temp_dir: str):
        self.config = config
        self.tmdb = tmdb
        self.table_ingestor = table_ingestor
        self.utc_now = datetime.now(UTC)
        self.temp_dir = temp_dir

    def run(self):
        log.info(f'Starting job for {self.config.table_name}')
        movies_data = self.tmdb.fetch_movies(start_page=1, max_pages=2)

        df = pl.DataFrame(movies_data)
        log.info(f'Fetched {df.height} movies')
        df = self.transform_data(df)
        dump_path = f'{self.temp_dir}/{self.config.table_name.replace(".", "_")}.csv'
        df.write_csv(dump_path, separator='|', null_value=None)
        self.create_temp_table(df)
        self.table_ingestor.execute(dump_path)

    def create_temp_table(self, df: pl.DataFrame):
        log.info(f'Creating temporary table {self.table_ingestor.temp_table}')
        df = df.remove()
        with self.table_ingestor.engine.connect() as conn:
            df.write_database(self.table_ingestor.temp_table, connection=conn, if_table_exists='replace')

    @staticmethod
    def transform_data(df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns([pl.col('release_date').cast(pl.Date)])
        df = df.with_columns(pl.col('genre_ids').list.eval(pl.element().cast(pl.Utf8)).list.join(',').alias('genre_ids'))
        return df


def main(config_uri: str = f'{CONFIG_URI}/tmdb_config.json'):
    config_repo = ConfigFactory.from_uri(config_uri)
    config = config_repo.get(JobConfig)
    secret_manager = SecretManager()
    temp_dir = f'{TEMP_PATH}/themoviedb'
    conn = ConnectionFactory.from_uri(str(config.db_uri))
    # Ensure temp directory exists
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

        tmdb = TMDB(secret_manager.get_secret('themoviedb_api').get('token'), temp_dir)

        tmdb_job = TMDBJob(config=config, tmdb=tmdb, table_ingestor=table_ingestor, temp_dir=temp_dir)
        tmdb_job.run()

    # cleaning up temp directory
    if os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir)


if __name__ == '__main__':
    main()
