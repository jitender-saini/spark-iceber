from abc import ABC, abstractmethod
from datetime import datetime

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import TextClause

from util.logging import get_logger

log = get_logger(__name__)


class TableIngestor(ABC):
    def __init__(
        self,
        engine: Engine,
        table: str,
        load_timestamp: datetime,
        primary_keys: list[str],
        range_column: str,
        temp_schema: str | None = None,
    ):
        self.engine = engine
        self.table = table
        schema, table_name = _schema_and_table(table)
        self.temp_table = f'{temp_schema or schema}.{table_name}_temp'
        self.load_timestamp = load_timestamp
        self.primary_keys = primary_keys
        self.range_column = range_column

    def execute(self, dump_path: str) -> None:
        log.info(f'Ingesting dump from {dump_path} to {self.table}')
        self._ingest_dump_to_temp_table(dump_path)
        log.info(f'Data ingested to temporary table {self.temp_table}')
        self._copy_from_temp_to_destination_table()
        log.info(f'Data copied from temporary table {self.temp_table} to {self.table}')

    def _ingest_dump_to_temp_table(self, dump_path: str) -> None:
        ingest_stmts = self._ingest_to_temp_table(dump_path)
        statements = [
            text(f'DROP TABLE IF EXISTS {self.temp_table}'),
            *ingest_stmts,
            text(f'ALTER TABLE {self.temp_table} ADD load_timestamp TIMESTAMP NULL'),
            text(f"UPDATE {self.temp_table} SET load_timestamp = '{self.load_timestamp.isoformat()}'"),
            text('COMMIT;'),
        ]

        with self.engine.connect() as conn:
            for statement in statements:
                conn.execute(statement)

    @abstractmethod
    def _ingest_to_temp_table(self, dump_path: str) -> list[TextClause]:
        pass

    def _copy_from_temp_to_destination_table(self) -> None:
        columns = ', '.join(_quote(self._get_column_names()))
        statements = [
            text(f'CREATE TABLE IF NOT EXISTS {self.table} AS SELECT * FROM {self.temp_table} WHERE 1=0'),
            text(self._create_delete_query()),
            text(f"""
                INSERT INTO {self.table} ({columns}) 
                    WITH numbered_duplicates AS (
                        SELECT {columns}, 
                            ROW_NUMBER() OVER (
                                PARTITION BY {', '.join(_quote(self.primary_keys))} ORDER BY {_quote(self.range_column)} DESC
                            ) AS row_num 
                        FROM {self.temp_table}
                    )
                    SELECT {columns} 
                    FROM numbered_duplicates 
                    WHERE row_num = 1"""),
            text(f'DROP TABLE {self.temp_table}'),
            text('COMMIT;'),
        ]
        with self.engine.begin() as conn:
            for statement in statements:
                conn.execute(statement)

    def _get_column_names(self) -> list[str]:
        schema, table_name_without_schema = _schema_and_table(self.temp_table)
        inspector = inspect(self.engine)
        return [col['name'] for col in inspector.get_columns(table_name_without_schema, schema=schema)]

    def _create_delete_query(self) -> str:
        casted_keys = [f"COALESCE(CAST({key} AS VARCHAR), '')" for key in _quote(self.primary_keys)]
        key_concat = " || '-' || ".join(casted_keys)
        return f"""
            DELETE FROM {self.table} 
            WHERE {key_concat} IN (
                SELECT DISTINCT {key_concat} FROM {self.temp_table}
            )"""


def _quote(name: str | list[str]) -> str | list[str]:
    if isinstance(name, str):
        return f'"{name}"'
    return [f'"{n}"' for n in name]


def _schema_and_table(table: str) -> tuple[str | None, str]:
    table_parts = table.split('.')
    return table_parts[0] if len(table_parts) == 2 else None, table_parts[-1]  # noqa: PLR2004


class DuckDBTableIngestor(TableIngestor):
    def __init__(
        self,
        engine: Engine,
        table: str,
        load_timestamp: datetime,
        primary_keys: list[str],
        range_column: str,
    ):
        super().__init__(engine, table, load_timestamp, primary_keys, range_column)

    def _ingest_to_temp_table(self, dump_path: str) -> list[TextClause]:
        return [
            text(f"""
                    CREATE TABLE {self.temp_table} 
                    AS FROM read_parquet(['{dump_path}/*.parquet']);
            """),
        ]
