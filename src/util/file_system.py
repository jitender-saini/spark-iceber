import os
from urllib.parse import urlparse

import polars as pl
from fsspec import filesystem

from util.local_env import MINIO_ACCESS_KEY_ID, MINIO_HOST, MINIO_PORT, MINIO_SECRET_ACCESS_KEY


class FileSystemType:
    FILE = 'file'
    S3 = 's3'
    MINIO = 'minio'


class DataFrameFormat:
    CSV = 'csv'
    PARQUET = 'parquet'


class FileSystem:
    def __init__(self, file_system_uri: str):
        parsed_uri = urlparse(file_system_uri)
        self.fs_type = parsed_uri.scheme
        if self.fs_type == FileSystemType.FILE:
            self.base_path = parsed_uri.path
            self.fs = filesystem('file')
        elif self.fs_type == FileSystemType.S3:
            self.base_path = os.path.join(parsed_uri.hostname, parsed_uri.path.lstrip('/'))
            self.fs = filesystem('s3')
        elif self.fs_type == FileSystemType.MINIO:
            self.base_path = os.path.join(parsed_uri.hostname, parsed_uri.path.lstrip('/'))
            self.fs = filesystem(
                's3',
                key=MINIO_ACCESS_KEY_ID,
                secret=MINIO_SECRET_ACCESS_KEY,
                client_kwargs={'endpoint_url': f'http://{MINIO_HOST}:{MINIO_PORT}'},
            )
        else:
            raise ValueError(f'Unsupported file system type: {self.fs_type}')

    def write_df(self, file_path: str, df: pl.DataFrame, df_format: DataFrameFormat) -> None:
        absolute_path = self._absolute_path(file_path)
        if self.fs_type == FileSystemType.FILE:
            os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
        with self.fs.open(absolute_path, mode='wb') as f:
            if df_format == DataFrameFormat.CSV:
                df.write_csv(f)
            elif df_format == DataFrameFormat.PARQUET:
                df.write_parquet(f)

    def write(self, file_path: str, data: str | bytes) -> None:
        absolute_path = self._absolute_path(file_path)
        if self.fs_type == FileSystemType.FILE:
            os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
        mode = 'wb' if isinstance(data, bytes) else 'w'
        with self.fs.open(absolute_path, mode) as f:
            f.write(data)

    def read_bytes(self, file_path: str) -> bytes:
        absolute_path = self._absolute_path(file_path)
        with self.fs.open(absolute_path, 'rb') as f:
            return f.read()

    def read_text(self, file_path: str) -> str:
        absolute_path = self._absolute_path(file_path)
        with self.fs.open(absolute_path, 'r') as f:
            return f.read()

    def exists(self, file_path: str) -> bool:
        return self.fs.exists(self._absolute_path(file_path))

    def _absolute_path(self, file_path: str) -> str:
        return os.path.join(self.base_path, file_path.lstrip('/'))
