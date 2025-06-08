from collections.abc import Generator
from pathlib import Path

import pytest
from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class MixpanelSettings(BaseSettings):
    service_account_secret: SecretStr = Field(..., description='Mixpanel service account secret', alias='MIXPANEL_SERVICE_ACCOUNT_SECRET')

    model_config = SettingsConfigDict(case_sensitive=True, extra='ignore', populate_by_name=True)


class RedshiftSettings(BaseSettings):
    password: SecretStr = Field(..., description='Integration test user password', alias='REDSHIFT_PASSWORD')
    ssh_tunnel_private_key: SecretStr = Field(
        ..., description='Private key to create SSH tunnel to Redshift through AWS Bastion', alias='REDSHIFT_SSH_TUNNEL_PRIVATE_KEY'
    )

    model_config = SettingsConfigDict(case_sensitive=True, extra='ignore', populate_by_name=True)


class GoogleSheetsSettings(BaseSettings):
    api_secret: SecretStr = Field(..., description='Google Sheets API secret', alias='GOOGLE_SHEETS_API_SECRET')
    model_config = SettingsConfigDict(case_sensitive=True, extra='ignore', populate_by_name=True)


class Settings(BaseSettings):
    mixpanel: MixpanelSettings
    redshift: RedshiftSettings
    google_sheets: GoogleSheetsSettings

    model_config = SettingsConfigDict(case_sensitive=True, extra='ignore')

    @classmethod
    def from_env_file(cls, env_file: Path | str) -> 'Settings':
        mixpanel = MixpanelSettings(_env_file=env_file)
        redshift = RedshiftSettings(_env_file=env_file)
        google_sheets = GoogleSheetsSettings(_env_file=env_file)
        return cls(mixpanel=mixpanel, redshift=redshift, google_sheets=google_sheets)


@pytest.fixture(scope='session')
def test_settings(env_file_path: str) -> Generator['Settings', None, None]:
    try:
        settings = Settings.from_env_file(env_file_path)
        yield settings
    except ValidationError as e:
        pytest.exit(f'Error: test environment validation failed:\n{e!s}')
