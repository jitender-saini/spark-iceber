import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class Metadata:
    schema_name: str
    table_name: str
    is_active: bool
    bookmark: str
    range_column: str | None
    primary_keys: list[str] | None
    source_schema_table: str | None
    bucket: str | None
    create_table_query: str | None
    partition_type: str | None
    partition_column: str | None
    load_type: str | None  # full_refresh or incremental
    url: str | None

    @property
    def full_table_name(self) -> str:
        return f'{self.schema_name}.{self.table_name}'

    @property
    def temp_full_table_name(self):
        return f'{self.full_table_name}_temp'

    @property
    def temp_table_name(self):
        return f'{self.table_name}_temp'


def convert_to_metadata(record: dict) -> Metadata:
    schema_name, table_name = record['table_name'].split('.')
    return Metadata(
        schema_name=schema_name,
        table_name=table_name,
        bookmark=record['bookmark'],
        range_column=record.get('range_column'),
        primary_keys=record['primary_key'].split(',') if 'primary_key' in record else None,
        source_schema_table=record.get('source_schema_table'),
        is_active=record.get('is_active'),
        bucket=record.get('s3_path'),
        create_table_query=record.get('create_table_query'),
        partition_type=record.get('partition_type'),
        partition_column=record.get('partition_column'),
        load_type=record.get('load_type'),
        url=record.get('url'),
    )


class MetadataRepository(ABC):
    @staticmethod
    def from_uri(uri: str):
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == 'file':
            return FileMetadataRepository(parsed_uri.path)
        else:
            raise ValueError(f'Unsupported URI scheme: {parsed_uri.scheme}')

    @abstractmethod
    def get_by_job_name(self, job_name: str) -> list[Metadata]:
        pass

    @abstractmethod
    def get_by_name(self, table_name: str) -> Metadata | None:
        pass

    @abstractmethod
    def update_bookmark(self, table_name: str, value: str) -> None:
        pass

    @abstractmethod
    def update_property(self, table_name: str, key: str, value: str, value_type: str) -> None:
        pass


class FileMetadataRepository(MetadataRepository):
    def __init__(self, file_name: str):
        self.file_name = file_name

    def get_by_job_name(self, job_name: str) -> list[Metadata]:
        with open(self.file_name) as file:
            items = json.load(file)
        filtered_data = [item for item in items if item.get('job_name') == job_name]
        return [convert_to_metadata(item) for item in filtered_data]

    def get_by_name(self, table_name: str) -> Metadata | None:
        with open(self.file_name) as file:
            items = json.load(file)
        for item in items:
            if item.get('table_name') == table_name:
                return convert_to_metadata(item)
        return None

    def update_bookmark(self, table_name: str, value: str) -> None:
        with open(self.file_name) as file:
            items = json.load(file)
        for item in items:
            if item.get('table_name') == table_name:
                item['bookmark'] = value
        with open(self.file_name, 'w') as file:
            json.dump(items, file, indent=2)

    def update_property(self, table_name: str, key: str, value: str, value_type: str) -> None:
        with open(self.file_name) as file:
            items = json.load(file)
        if value_type == 'bool':
            value = value.lower() == 'true'
        elif value_type == 'int':
            value = int(value)

        for item in items:
            if item.get('table_name') == table_name:
                item[key] = value
        with open(self.file_name, 'w') as file:
            json.dump(items, file, indent=2)

    def _read_metadata(self):
        with open(self.file_name) as file:
            return json.load(file)

    def _write_metadat(self, items):
        with open(self.file_name, 'w') as file:
            json.dump(items, file)
