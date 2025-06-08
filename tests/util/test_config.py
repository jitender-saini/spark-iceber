import json
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import pytest
from boto3.dynamodb.types import TypeDeserializer
from fixtures.utc import datetime_utc
from pydantic import BaseModel
from pytest import FixtureRequest

from util.config import ConfigFactory, ConfigRepository
from util.logging import get_logger

log = get_logger(__name__)


class FloatDeserializer(TypeDeserializer):
    def _deserialize_n(self, value):
        return float(value)


class TestConfig(BaseModel):
    name: str
    value: int
    nested: dict[str, Any]
    bookmark: datetime = None


config = {
    'name': 'test-config',
    'value': 42,
    'nested': {'key1': 'value1', 'key2': 123, 'key3': True},
    'bookmark': '2021-02-03T04:05:06.123Z',
}


@pytest.fixture(params=['file', 'dynamodb'])
def config_repository(request: FixtureRequest, tmp_path: Path, test_function_id: str) -> Generator[ConfigRepository, None, None]:
    match request.param:
        case 'file':
            temp_file = tmp_path / 'config'
            temp_file.write_text(json.dumps(config))
            yield ConfigFactory.from_uri(f'file://{temp_file}')
        case 'dynamodb':
            table = boto3.resource('dynamodb', region_name='eu-central-1').Table('integration-test')
            table.meta.client.deserializer = FloatDeserializer()
            config_name = f'test-config-{test_function_id}'
            expire_at = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
            table.put_item(Item={'config_name': config_name, 'expire_at': expire_at} | config)
            yield ConfigFactory.from_uri(f'dynamodb://integration-test/{config_name}')
            try:
                table.delete_item(Key={'config_name': config_name})
            except Exception as e:
                log.warning(f'Error during DynamoDB integration-test table cleanup: {e}')


@pytest.mark.integration
def test_get_update(config_repository: ConfigRepository):
    assert config_repository.get(TestConfig) == TestConfig(
        name='test-config',
        value=42,
        nested={'key1': 'value1', 'key2': 123, 'key3': True},
        bookmark=datetime_utc(2021, 2, 3, 4, 5, 6, 123000),
    )

    config_repository.update(datetime_utc(2022, 3, 4, 5, 6, 7, 234))

    assert config_repository.get(TestConfig) == TestConfig(
        name='test-config',
        value=42,
        nested={'key1': 'value1', 'key2': 123, 'key3': True},
        bookmark=datetime_utc(2022, 3, 4, 5, 6, 7, 234),
    )
