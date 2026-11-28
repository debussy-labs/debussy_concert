# Changelog

All notable changes to Debussy are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [semantic versioning](https://semver.org/). Each change is described in detail in its pull request and in the [GitHub Releases](https://github.com/debussy-labs/debussy/releases).

## [Unreleased] - 2.0.0

Pre-releases: 2.0.0.dev1 (2026-11-27).

### Breaking changes

Each item has its migration step. The [migration guide](https://github.com/debussy-labs/debussy/blob/main/docs/migration.md) walks through them with examples.

- **The package is renamed from `debussy_concert` to `debussy`.** Install `debussy` and replace `debussy_concert` with `debussy` in your imports.
- **Environment variables use the prefix `DEBUSSY__`, not `DEBUSSY_CONCERT__`.** Rename them in your YAML files, for example `${DEBUSSY__DAGS_FOLDER}`.
- **Python 3.11+ and Airflow 3.4+ are required.** Upgrade your Airflow environment (on Cloud Composer, Composer 3 with Airflow 3).
- **Airflow, Google Cloud, dbt, MySQL and SFTP dependencies are extras.** Install the ones you use, for example `pip install "debussy[airflow,gcp,dbt]"`.
- **`debussy-airflow` and `airflow-dbt` are no longer dependencies.** Their operators and hooks are in `debussy.backends.airflow`. Uninstall them unless other code needs them.
- **`airflow_easy_setup` and the per-pipeline DAG files are removed.** Add one DAGs-folder file that calls `debussy.backends.airflow.load_dags`, and move `composition_type` and `os_env_prefix` into each composition YAML.
- **`dag_parameters.schedule_interval` is now `schedule`.** Rename the key in your YAML files and `ConfigDagParameters` calls.
- **Templates use Airflow 3 names.** Replace `execution_date` with `logical_date` and `next_execution_date` with `data_interval_end`, and `macros.ds_format(logical_date, ...)` with `logical_date.strftime(...)`.
- **Airflow code moved to `debussy.backends.airflow`.** Import `AirflowService` from `debussy.backends.airflow.service` instead of `debussy_concert.core.service.workflow.airflow`.
- **Some internal modules moved.** If your code imports them, import `StartMotif` and `EndMotif` from `debussy.core.motif.lifecycle`, the BigQuery job builders (`TableReference`, `BigQueryTimePartitioning`, `BigQueryJobMixin`) from `debussy.core.service.lakehouse.bigquery_job`, and `BigQueryQueryJobMotif` from `debussy.pipeline.reverse_etl.motif.bigquery_query_job`.
- **The Airflow `DummyMotif` is removed.** It had no users; use an `EndMotif` or a motif of your own.

### Added

- `debussy.backends.airflow.load_dags`, one loader for YAML pipelines (`composition_type` and `os_env_prefix` keys) and pipelines built in Python. It logs and skips a pipeline that fails to build; `strict=True` raises.
- An engine-neutral task contract in `debussy.core.tasks`: `TaskSpec` (with `always_run` and `result_of()`), `RunContext`, `Capability`, `ActionRegistry`. A motif can define `tasks()` instead of overriding `build()`, and the Airflow backend builds those tasks as operators (`debussy.backends.airflow.tasks`).
- Characterization tests that compare every example DAG with a committed snapshot, and CI on GitHub Actions.
- A release workflow that publishes to PyPI with Trusted Publishing and creates a GitHub Release.
- Jinja2 as a core dependency.

### Changed

- Every pipeline motif (BigQuery, storage, dbt and Dataproc export) and the start and end motifs describe their work with `tasks()` and named actions. They import without Airflow.
- Only `debussy.backends` and the pipeline compositions import Airflow. The core, configs, phrases and motifs import without it.
- The `bigquery_ingestion_incr` example builds its partition with `logical_date.strftime('%Y%m%d%H')`.

### Fixed

- Manual runs without a logical date (the Airflow 3 default) render their templates, as if triggered at their `run_after` time.
- Parsing the DAGs no longer needs any connection: `GCSHook` connects on first use.
- The Dataproc tasks use the environment's `data_lakehouse_connection_id` instead of `google_cloud_default`.
- `CreateExternalBigQueryTableMotif` works again, with `BigQueryCreateTableOperator`.
- A `ConfigRdbmsDataIngestion` built in Python gets the default PySpark script, which fixes the Python-API ("yamless") example.
- The dbt paths of the `sakila_transformation` example.

## [1.2.0]

### Added

- `airflow_easy_setup` and Dataproc Serverless support.
- A package summary.

### Fixed

- Packaging: 1.0.0 and 1.1.0 shipped without their subpackages.
- `debussy-airflow` 0.0.7+ is required, so the package installs again.

## [1.1.0]

### Added

- Dataproc Serverless support.

### Fixed

- BigQuery load with a schema.

## [1.0.0]

### Changed

- Improved folder structure and dependencies.

## [0.1.0]

First release.

[Unreleased]: https://github.com/debussy-labs/debussy/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/debussy-labs/debussy/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/debussy-labs/debussy/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/debussy-labs/debussy/releases/tag/v1.0.0
