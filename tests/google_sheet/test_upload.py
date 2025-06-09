from decimal import Decimal

import sqlalchemy as sa

from google_sheet.uploader import JobConfig, UploadGoogleSheetJob
from util.config import InMemoryBookmarkUpdater
from util.connection_factory import DuckDBConnection
from util.google_sheet import FakeGoogleSheet

TABLE_NAME = 'gs_temp'
SCHEMA_NAME = 'main'
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1AQ5M7bK9ceHkLBu-UrtrMc9KuJNRXibVFW2V0v7vk4I/edit'
WORKSHEET_NAME = 'test-worksheet'
job_config = JobConfig


def test_upload_google_sheet_job(duckdb_connection: DuckDBConnection):
    data = [
        {'name': 'Alice', 'score': Decimal('85.5'), 'date': '2024-02-15'},
        {'name': 'Bob', 'score': Decimal('92.3'), 'date': '2024-02-16'},
        {'name': 'Charlie', 'score': Decimal('78.9'), 'date': '2024-02-17'},
    ]
    create_data_table(duckdb_connection, data, TABLE_NAME)
    google_sheet = FakeGoogleSheet()
    uploader = UploadGoogleSheetJob(
        google_sheet=google_sheet,
        connection=duckdb_connection,
        update_bookmark=InMemoryBookmarkUpdater,
    )

    uploader.run()

    worksheet_data = google_sheet.get_worksheet(SHEET_URL, WORKSHEET_NAME)
    assert worksheet_data[1:] == [[d['name'], d['score'], d['date']] for d in data]


def test_upload_google_sheet_job_without_where_clause(duckdb_connection: DuckDBConnection):
    data = [
        {'name': 'Alice', 'score': Decimal('85.5'), 'date': '2024-02-15'},
        {'name': 'Bob', 'score': Decimal('92.3'), 'date': '2024-02-16'},
        {'name': 'Charlie', 'score': Decimal('78.9'), 'date': '2024-02-17'},
    ]
    create_data_table(duckdb_connection, data, TABLE_NAME)
    google_sheet = FakeGoogleSheet()
    uploader = UploadGoogleSheetJob(
        google_sheet=google_sheet,
        connection=duckdb_connection,
        sheet_url=SHEET_URL,
        worksheet_name=WORKSHEET_NAME,
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        columns=['name', 'score', 'date'],
    )

    uploader.run()

    worksheet_data = google_sheet.get_worksheet(SHEET_URL, WORKSHEET_NAME)
    assert worksheet_data[1:] == [[d['name'], d['score'], d['date']] for d in data]


def test_upload_google_sheet_job_without_limit(duckdb_connection: DuckDBConnection):
    data = [
        {'name': 'Alice', 'score': Decimal('85.5'), 'date': '2024-02-15'},
        {'name': 'Bob', 'score': Decimal('92.3'), 'date': '2024-02-16'},
        {'name': 'Charlie', 'score': Decimal('78.9'), 'date': '2024-02-17'},
        {'name': 'David', 'score': Decimal('88.1'), 'date': '2024-02-18'},
        {'name': 'Eve', 'score': Decimal('95.0'), 'date': '2024-02-19'},
    ]
    create_data_table(duckdb_connection, data, TABLE_NAME)
    google_sheet = FakeGoogleSheet()
    uploader = UploadGoogleSheetJob(
        google_sheet=google_sheet,
        connection=duckdb_connection,
        sheet_url=SHEET_URL,
        worksheet_name=WORKSHEET_NAME,
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        columns=['name', 'score', 'date'],
        where_clause="where score > 85 and date > '2024-02-17'",
    )

    uploader.run()

    worksheet_data = google_sheet.get_worksheet(SHEET_URL, WORKSHEET_NAME)
    expected_data = [[d['name'], d['score'], d['date']] for d in data if d['score'] > 85 and d['date'] > '2024-02-17']
    assert worksheet_data[1:] == expected_data


def create_data_table(duckdb_connection: DuckDBConnection, data, table_name: str):
    with duckdb_connection.get_sqlalchemy_engine() as engine:
        metadata = sa.MetaData()
        source = sa.Table(
            table_name,
            metadata,
            sa.Column('name', sa.String),
            sa.Column('score', sa.Numeric(3, 1)),
            sa.Column('date', sa.Date),
            schema=SCHEMA_NAME,
        )
        metadata.create_all(engine)
        engine.execute(source.insert(), data)
