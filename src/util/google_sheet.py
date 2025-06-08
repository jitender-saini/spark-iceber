import re
from abc import ABC, abstractmethod

import gspread
import polars as pl
from gspread.utils import ValueInputOption
from oauth2client.service_account import ServiceAccountCredentials

from util.lazy import lazy
from util.logging import get_logger

log = get_logger(__name__)


class GoogleSheetFactory:
    @classmethod
    def from_credential_json(cls, creds: dict) -> 'GoogleSheet':
        return RemoteGoogleSheet(creds)


class GoogleSheet(ABC):
    @abstractmethod
    def get_worksheet(self, url: str, worksheet_name: str) -> list[list[str]]:
        pass

    @abstractmethod
    def update_worksheet(self, url: str, worksheet_name: str, df: pl.DataFrame) -> None:
        pass


class RemoteGoogleSheet(GoogleSheet):
    def __init__(self, secret: dict):
        self._client = self._auth(secret)

    @staticmethod
    @lazy
    def _auth(secret: dict) -> gspread.Client:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(secret, scopes)
        return gspread.authorize(credentials)

    def _get_spreadsheet(self, url: str) -> gspread.Spreadsheet:
        return self._client().open_by_key(self._get_file_id_from_url(url))

    def get_worksheet(self, url: str, worksheet_name: str) -> list[list[str]]:
        spreadsheet = self._get_spreadsheet(url)
        worksheet_name = worksheet_name if worksheet_name else spreadsheet.sheet1.title
        worksheet = spreadsheet.worksheet(worksheet_name)
        return worksheet.get_all_values()

    def update_worksheet(self, url: str, worksheet_name: str, df: pl.DataFrame) -> None:
        spreadsheet = self._get_spreadsheet(url)
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
        formatted_df = self._format_temporal_columns(df)
        worksheet.update([formatted_df.columns, *formatted_df.rows()], value_input_option=ValueInputOption.user_entered)

    @staticmethod
    def _get_file_id_from_url(url: str) -> str:
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
        if match:
            return match.group(1)
        raise ValueError('Invalid Google Sheet URL')

    @staticmethod
    def _format_temporal_columns(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            [pl.col(col).dt.strftime('%Y-%m-%d') for col in df.select(pl.col(pl.Date)).columns],
        ).with_columns(
            [pl.col(col).dt.strftime('%Y-%m-%d %H:%M:%S') for col in df.select(pl.col(pl.Datetime)).columns],
        )
