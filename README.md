
# ETL jobs

## Local development environment setup

The setup also includes a Minio container that provides local, S3-compatible storage for data files and the Iceberg data lake.
This project uses UV for dependency management. All dependencies are specified in the `pyproject.toml` file.

We use the `just` command runner to simplify local job execution with Docker Compose. To see all available commands, run:

```shell
just
```

The following sections will explain the setup and execution steps in detail.

### Install CLI dependencies (macOS)

Install [Homebrew](https://brew.sh/) and run:

```shell
brew bundle
```

### Configure pre-commit hooks

We use [Ruff](https://github.com/astral-sh/ruff) for linting and formatting and [pre-commit](https://pre-commit.com/) to run Ruff in a Git pre-commit hook.

To set up pre-commit hooks, run:

```shell
pre-commit install
```


### direnv

You can leverage [direnv](https://direnv.net/) to automatically load environment variables from the `.env` file
when entering the project directory.

#### Lint / format with Ruff

Install [Ruff](https://plugins.jetbrains.com/plugin/20574-ruff) plugin to use Ruff for code inspections and as IntelliJ formatter.

To configure the Ruff plugin, go to _Settings / Tools / Ruff_:
- **Set [ruff.toml](./ruff.toml) as the Ruff config file**
- Make sure that _Run ruff when Reformat Code_ and _Use ruff format (Experimantal)_ are checked.
- Double-check the path to the Ruff executable


## Development

### Minio

#### MinIO UI Access

You can access the MinIO UI at http://localhost:9001 with the following default credentials:

- Username: `minioadmin`
- Password: `minioadmin`

#### Iceberg datalake bucket

To configure Minio as your Iceberg datalake storage, create a bucket for it (e.g. `datalake`):
access the Minio UI, navigate to _Administrator > Buckets_, click _Create Bucket_, and name it `datalake`.

#### Programmatic access (using MinIO credentials)

For programmatic interaction with MinIO, pass the following arguments to the boto3 client constructor: `endpoint_url='http://minio:9000/'`.
Use the same credentials as for the MinIO UI:

- `AWS_ACCESS_KEY_ID`: `minioadmin`
- `AWS_SECRET_ACCESS_KEY`: `minioadmin`

### Jupyter Lab

Run:

```shell
just jlab
```

Open the provided link in your web browser.

The Jupyter IPython kernel has access to all dependencies, including Spark and Iceberg.
The `scripts` and `src` directories are automatically added to the Python path.

The Jupyter kernel uses the [autoreload](https://ipython.readthedocs.io/en/stable/config/extensions/autoreload.html) plugin,
which automatically reloads all modules before code execution.


### Run jobs with python-submit

Run:

```bash
just python-submit <script> [job arguments]
```

- `<script>`: the path to your Spark application's main Python script.

`spark-submit` command automatically adds job dependencies from the `src` directory

### Run jobs with spark-submit

Run:

```bash
just spark-submit <script> [job arguments]
```

- `<script>`: the path to your Spark application's main Python script.

`spark-submit` command automatically adds job dependencies from the `src` directory

#### Iceberg tables

Iceberg catalog is configured in [SparkSessionFactory](src/util/spark_session_factory.py) as `iceberg_catalog`.

In Spark SQL queries or statements, we need to prefix We need to prefix the schema and table name with the Iceberg catalog name, so Spark
knows where to look for the table (e.g. `SELECT * FROM iceberg_catalog.stage_webtracking.event`).