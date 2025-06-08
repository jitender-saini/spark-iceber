from pathlib import Path

import pytest

from util.connection_factory import ConnectionFactory, DuckDBConnection


@pytest.fixture
def duckdb_connection(tmp_path: Path, test_function_id: str) -> DuckDBConnection:
    db = tmp_path / f'db-{test_function_id}'
    return ConnectionFactory.from_uri(f'duckdb://{db}')
