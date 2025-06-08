from io import BytesIO, StringIO

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pytest import FixtureRequest

from util.file_system import DataFrameFormat, FileSystem

df = pl.DataFrame({'col1': [1, 2], 'col2': [3, 4]})


@pytest.fixture(params=['file', 'minio', 's3'])
def file_system(request: FixtureRequest) -> FileSystem:
    match request.param:
        case 'file':
            return request.getfixturevalue('local_file_system')
        case 'minio':
            return request.getfixturevalue('minio_file_system')
        case 's3':
            return request.getfixturevalue('s3_file_system')


@pytest.mark.integration
def test_write_read(file_system: FileSystem):
    file_path = '/dir/test.txt'
    data = 'Hello world!'
    file_system.write(file_path, data)
    assert file_system.exists(file_path)

    assert file_system.read_text(file_path) == data
    assert file_system.read_bytes(file_path).decode() == data


@pytest.mark.integration
def test_write_read_df_csv(file_system: FileSystem):
    file_path = '/dir/test.csv'
    file_system.write_df(file_path, df, DataFrameFormat.CSV)
    assert file_system.exists(file_path)

    downloaded_df = pl.read_csv(StringIO(file_system.read_text(file_path)))
    assert_frame_equal(downloaded_df, df)


@pytest.mark.integration
def test_write_read_df_parquet(file_system: FileSystem):
    file_path = '/dir/test.parquet'
    file_system.write_df(file_path, df, DataFrameFormat.PARQUET)
    assert file_system.exists(file_path)

    downloaded_df = pl.read_parquet(BytesIO(file_system.read_bytes(file_path)))
    assert_frame_equal(downloaded_df, df)
