import json
import os

from util.local_env import SECRETS_PATH


class SecretManager:
    def __init__(self):
        self.secret_file = SECRETS_PATH
        self.secrets = self._load_secrets()

    def _load_secrets(self) -> dict:
        if not os.path.exists(self.secret_file):
            raise FileNotFoundError(f'Secret file {self.secret_file} does not exist.')

        with open(self.secret_file) as file:
            return json.load(file)

    def get_secret(self, key: str) -> dict:
        return self.secrets.get(key, {})
