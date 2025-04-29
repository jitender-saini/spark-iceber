import os

CATALOG_URI = os.environ.get('CATALOG_URI', 'jdbc://minioadmin:minioadmin@minio:9000/datalake')
CONFIG_URI = os.environ.get('CONFIG_URI', 'file:///home/iceberg/app/.metadata')
DUCKDB_URI = os.environ.get('DUCKDB_URI', 'duckdb:///home/iceberg/app/.db/db.duckdb')
FILE_SYSTEM_URI = os.environ.get('FILE_SYSTEM_URI', 'file:///home/iceberg/app/.data')
TEMP_PATH = os.environ.get('TEMP_PATH', '/home/iceberg/app/.data')

GOOGLE_SHEET_SECRET_PATH = os.environ.get('GOOGLE_SHEET_SECRET_PATH', '/home/iceberg/app/.catalog/gs_secret.json')

MINIO_HOST = 'minio'
MINIO_PORT = 9000
MINIO_ACCESS_KEY_ID = 'minioadmin'
MINIO_SECRET_ACCESS_KEY = 'minioadmin'
