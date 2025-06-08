from pathlib import Path

import boto3
import pytest
from fsspec import AbstractFileSystem, filesystem

from util.file_system import FileSystem
from util.local_env import MINIO_ACCESS_KEY_ID, MINIO_HOST, MINIO_PORT, MINIO_SECRET_ACCESS_KEY
from util.logging import get_logger

log = get_logger(__name__)


@pytest.fixture
def minio_bucket(test_function_id: str) -> str:
    s3_client = boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_HOST}:{MINIO_PORT}',
        aws_access_key_id=MINIO_ACCESS_KEY_ID,
        aws_secret_access_key=MINIO_SECRET_ACCESS_KEY,
    )
    bucket_name = f'test-{test_function_id}'

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        yield bucket_name
    except Exception as e:
        log.error(f'Error creating Minio bucket {bucket_name}: {e}')
    finally:
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in response:
                for obj in response['Contents']:
                    s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

            s3_client.delete_bucket(Bucket=bucket_name)
        except Exception as e:
            log.warning(f'Error during Minio bucket {bucket_name} cleanup: {e}')


@pytest.fixture
def local_file_system(tmp_path: Path) -> FileSystem:
    return FileSystem(f'file://{tmp_path}')


@pytest.fixture
def minio_file_system(minio_bucket: str) -> FileSystem:
    return FileSystem(f'minio://{minio_bucket}')


@pytest.fixture
def s3_file_system(test_function_id: str) -> FileSystem:
    s3_uri = f's3://airup-test-data-dev-eu-central-1/integration-test/test-{test_function_id}'
    yield FileSystem(s3_uri)
    try:
        recursive_remove(filesystem('s3'), s3_uri)
    except Exception as e:
        log.warning(f'Error during S3 folder {s3_uri} cleanup: {e}')


def recursive_remove(fs: AbstractFileSystem, path: str) -> None:
    if not fs.exists(path):
        return

    if fs.isfile(path):
        fs.rm(path)
        return

    for entry in fs.ls(path, detail=True):
        entry_path = entry['name']
        if entry['type'] == 'file':
            fs.rm(entry_path)
        else:
            recursive_remove(fs, entry_path)
