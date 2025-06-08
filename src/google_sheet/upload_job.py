import sys

from google_sheet.uploader import JobConfig, UploadJob
from util.config import ConfigFactory
from util.connection_factory import ConnectionFactory
from util.google_sheet import GoogleSheetFactory
from util.logging import configure_logging, get_logger, log_execution_time
from util.secret_manager import SecretManager

configure_logging()
log = get_logger(__name__)


@log_execution_time(log)
def main(config_uri: str) -> None:
    config_repo = ConfigFactory.from_uri(config_uri)
    config = config_repo.get(JobConfig)
    secret = SecretManager().get_secret(config.gs_secret_name)
    google_sheet = GoogleSheetFactory.from_credential_json(secret)
    connection = ConnectionFactory.from_uri(str(config.duckdb_uri))

    job = UploadJob(
        google_sheet=google_sheet,
        connection=connection,
        config=config,
        update_bookmark=config_repo.update,
    )
    job.run()


if __name__ == '__main__':
    # TODO: Add error alert
    log.info('Starting uploader job, system args: %s', sys.argv)
    main(sys.argv[1])
