import json
from pathlib import Path
from typing import Any, Callable

import pytest

from util.local_env import CONFIG_URI


@pytest.fixture
def job_config(test_function_id: str) -> Callable[[dict[str, Any]], Path]:
    config_path = Path(CONFIG_URI) / f'test-{test_function_id}.json'

    def write_config(config: dict[str, Any]) -> Path:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        return config_path

    yield write_config

    if config_path.exists():
        config_path.unlink()
