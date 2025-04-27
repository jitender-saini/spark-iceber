from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from enum import Enum
from typing import Any
from urllib.parse import urlparse

import sqlalchemy as sa

from util.credentials import Credentials


class ConnectionFactory:
    @staticmethod
    def from_uri(uri: str) -> 'Connection':
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == 'postgres':
            creds = Credentials(parsed_uri.username, parsed_uri.password)
            return PostgresConnection(parsed_uri.hostname, parsed_uri.port, parsed_uri.path[1:], creds)
        elif parsed_uri.scheme == 'duckdb':
            return DuckDBConnection(parsed_uri.path)
        else:
            raise ValueError(f'Unsupported URI scheme: {parsed_uri.scheme}')


class ConnectionType(Enum):
    POSTGRES = 'postgres'
    DUCKDB = 'duckdb'


class Connection(ABC):
    @abstractmethod
    def type(self) -> ConnectionType:
        pass

    @abstractmethod
    def get_sqlalchemy_engine(self) -> sa.engine.base.Engine:
        pass


class PostgresConnection(Connection):
    def __init__(self, host: str, port: int, database: str, credentials: Credentials):
        self.host = host
        self.port = port
        self.database = database
        self.credentials = credentials

    @property
    def type(self) -> ConnectionType:
        return ConnectionType.POSTGRES

    @contextmanager
    def get_sqlalchemy_engine(self) -> Generator[sa.Engine, Any]:
        engine = self._create_engine()
        try:
            yield engine
        finally:
            engine.dispose()

    def _create_engine(self):
        url = f'postgresql://{self.credentials.username}:{self.credentials.password}@{self.host}:{self.port}/{self.database}'
        return sa.create_engine(url, pool_size=100, max_overflow=200, executemany_mode='batch', echo=False)


class DuckDBConnection(Connection):
    def __init__(self, path: str | None):
        self.path = path

    @property
    def type(self) -> ConnectionType:
        return ConnectionType.DUCKDB

    @contextmanager
    def get_sqlalchemy_engine(self) -> Generator[sa.Engine, Any]:
        engine = self._create_engine()
        try:
            yield engine
        finally:
            engine.dispose()

    def _create_engine(self):
        if self.path:
            return sa.create_engine(f'duckdb:///{self.path}')
        return sa.create_engine('duckdb:///:memory:')
