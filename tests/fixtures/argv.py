from typing import Callable

import pytest


@pytest.fixture
def mock_argv(monkeypatch) -> Callable[[list[str]], None]:
    return lambda args: monkeypatch.setattr('sys.argv', ['script.py', *args])
