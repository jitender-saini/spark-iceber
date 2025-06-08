import json

import polars as pl
import pytest
from fixtures.settings import Settings
from polars.testing import assert_frame_equal
from pytest import FixtureRequest

from util.google_sheet import FakeGoogleSheet, GoogleSheet, RemoteGoogleSheet

SHEET_URL = 'https://docs.google.com/spreadsheets/d/1WtObv9nRjJKWc_d6RDsr8hHaOPfvwUORa8Yj0x-wK24/edit?gid=0#gid=0'
WORKSHEET_NAME = 'test-worksheet'


@pytest.fixture(params=['gsheet', 'fake'])
def google_sheet(request: FixtureRequest, test_settings: Settings) -> None | FakeGoogleSheet | GoogleSheet:
    match request.param:
        case 'gsheet':
            gs_secret = test_settings.google_sheets.api_secret.get_secret_value()
            return RemoteGoogleSheet(json.loads(gs_secret))
        case 'fake':
            return FakeGoogleSheet()


@pytest.mark.integration
def test_update_and_get_google_sheet(google_sheet: GoogleSheet):
    to_upload_df = (
        pl.DataFrame(
            {
                'name': ['Alice', 'Bob', 'Charlie'],
                'position': [1, 2, 3],
                'score': [85.5, 92.3, 78.9],
                'date': ['2024-02-15', '2024-02-16', '2024-02-17'],
                'datetime': ['2024-02-15 01:02:03', '2024-02-16 02:03:04', '2024-02-17 03:04:05'],
            }
        )
        .with_columns(pl.col('date').str.strptime(pl.Date, format='%Y-%m-%d'))
        .with_columns(pl.col('datetime').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S'))
    )

    google_sheet.update_worksheet(SHEET_URL, WORKSHEET_NAME, to_upload_df)
    data = google_sheet.get_worksheet(SHEET_URL, WORKSHEET_NAME)

    downloaded_df = pl.DataFrame(data[1:], schema=data[0], orient='row')
    downloaded_df = downloaded_df.with_columns(
        [
            pl.col('position').cast(pl.Int64),
            pl.col('score').cast(pl.Float64),
            pl.col('date').str.strptime(pl.Date, '%Y-%m-%d'),
            pl.col('datetime').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S'),
        ]
    )

    assert_frame_equal(to_upload_df, downloaded_df)
