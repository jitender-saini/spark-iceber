import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, Optional, TypeVar, Union
from urllib.parse import urlparse

from pydantic import BaseModel

from util.logging import get_logger

log = get_logger(__name__)

ConfigType = TypeVar('ConfigType', bound=BaseModel)
Bookmark = Union[datetime, int]


class ConfigReader(ABC):
    @abstractmethod
    def get(self, config_class: type[ConfigType]) -> ConfigType:
        pass


class BookmarkUpdater(ABC):
    @abstractmethod
    def update(self, new_bookmark: Bookmark) -> None:
        pass


class ConfigRepository(ConfigReader, BookmarkUpdater, ABC):
    pass


UpdateBookmark = Callable[[datetime], None]


class ConfigFactory:
    @staticmethod
    def from_uri(uri: str) -> ConfigRepository:
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == 'file':
            return FileConfigRepository(parsed_uri.path)
        else:
            raise ValueError(f'Unsupported URI scheme: {parsed_uri.scheme}')


class FileConfigRepository(ConfigRepository):
    def __init__(self, file_name: str):
        self.file_name = file_name

    def get(self, config_class: type[ConfigType]) -> ConfigType:
        config = self._read_config()
        return config_class(**config)

    def update(self, new_bookmark: Bookmark) -> None:
        config = self._read_config()
        self._write_config(config | {'bookmark': _serialize_bookmark(new_bookmark)})
        log.info(f'Bookmark updated to {new_bookmark}')

    def _read_config(self):
        with open(self.file_name) as file:
            return json.load(file)

    def _write_config(self, config):
        with open(self.file_name, 'w') as file:
            json.dump(config, file, indent=2)


def _serialize_bookmark(bookmark: Bookmark):
    return bookmark.isoformat() if isinstance(bookmark, datetime) else bookmark


def no_op_update_bookmark(_bookmark: Bookmark) -> None:
    pass


class InMemoryBookmarkUpdater:
    def __init__(self):
        self.bookmark: Optional[Bookmark] = None

    def update(self, new_bookmark: Bookmark) -> None:
        self.bookmark = new_bookmark
        log.info(f'Bookmark updated to {new_bookmark}')
