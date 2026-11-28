# Migrating from Debussy 1.x to 2.0

Debussy 2.0 renames the package, moves to Airflow 3 and replaces the per-pipeline DAG files with one loader. This guide walks through the changes a 1.x project needs, in the order you are likely to meet them. The [changelog](../CHANGELOG.md) lists every change.

## 1. Python and Airflow versions

Debussy 2.0 needs Python 3.11 or later and Airflow 3.4 or later (below 4). On Cloud Composer, use a Composer 3 environment that runs Airflow 3.

## 2. Install the new package

The package is now `debussy`, not `debussy_concert`, and the Airflow, Google Cloud, dbt, MySQL and SFTP dependencies are extras. Install the ones your pipelines use:

```bash
pip uninstall debussy_concert debussy-airflow airflow-dbt
pip install "debussy[airflow,gcp,dbt]"      # add mysql and sftp for reverse ETL
```

| Extra | Needed for |
| --- | --- |
| `airflow` | the Airflow backend (`debussy.backends.airflow`) |
| `gcp` | BigQuery, Dataproc and Cloud Storage motifs (includes `airflow`) |
| `dbt` | dbt transformation pipelines |
| `mysql` | reverse ETL to MySQL |
| `sftp` | reverse ETL to SFTP |

`debussy-airflow` and `airflow-dbt` are no longer dependencies: their operators and hooks now live in `debussy.backends.airflow`.

## 3. Rename imports and environment variables

| 1.x | 2.0 |
| --- | --- |
| `import debussy_concert...` | `import debussy...` |
| `${DEBUSSY_CONCERT__DAGS_FOLDER}` | `${DEBUSSY__DAGS_FOLDER}` |
| `${DEBUSSY_CONCERT__<NAME>}` | `${DEBUSSY__<NAME>}` |

Update the YAML files too: environment variables are read from them, for example in `raw_table_definition` paths.

## 4. Replace the DAG files with one loader

`airflow_easy_setup` and the one-file-per-pipeline setup are gone. Delete the per-pipeline DAG files and add one file to your DAGs folder:

```python
from debussy.backends.airflow import load_dags

load_dags(globals(), pipelines_dir="pipelines", environment_file="pipelines/environment.yaml")
```

Every YAML file under `pipelines_dir` with a top-level `composition_type` key becomes a DAG. The arguments of `airflow_easy_setup` move into the YAML file:

| `airflow_easy_setup(...)` argument | YAML key in the composition file |
| --- | --- |
| `composition_type="rdbms_ingestion"` | `composition_type: rdbms_ingestion` |
| `os_env_prefix="MYSQL_SAKILA_DAILY"` | `os_env_prefix: MYSQL_SAKILA_DAILY` |

Pipelines built in Python (with `ConfigRdbmsDataIngestion` and friends) go to the same loader, instead of `inject_dependencies` and `composition.play(...)`:

```python
load_dags(globals(), pipelines=[config_composition])
```

A pipeline that fails to build is logged and skipped, so the other DAGs of the file still load. Pass `strict=True` to raise instead, for example in CI.

## 5. Update DAG parameters and templates for Airflow 3

- `dag_parameters.schedule_interval` is now `dag_parameters.schedule`.
- Airflow 3 removed `execution_date`, `next_execution_date` and their relatives. Use `logical_date` and `data_interval_end`:

  | 1.x template | 2.0 template |
  | --- | --- |
  | `{{ execution_date.strftime('%Y%m%d') }}` | `{{ logical_date.strftime('%Y%m%d') }}` |
  | `{{ next_execution_date }}` | `{{ data_interval_end }}` |

- `macros.ds_format(logical_date, ...)` fails on Airflow 3, because a logical date no longer prints with a `T`. Use `logical_date.strftime(...)` instead.
- The `${<prefix>_WINDOW_START}` and `${<prefix>_WINDOW_END}` variables from `os_env_prefix` keep their names. They now expand to `logical_date` and `data_interval_end` templates.
- Manual runs triggered without a logical date (the Airflow 3 default) work: Debussy renders them as if triggered with the trigger time as logical date.

## 6. Custom code that imports Debussy internals

Skip this section if your pipelines only use YAML or the config classes.

| 1.x | 2.0 |
| --- | --- |
| `debussy_concert.core.service.workflow.airflow.AirflowService` | `debussy.backends.airflow.service.AirflowService` |
| `StartMotif`, `EndMotif` (Airflow default motifs) | `debussy.core.motif.lifecycle` |
| `TableReference`, `BigQueryTimePartitioning`, `BigQueryJobMixin` | `debussy.core.service.lakehouse.bigquery_job` |
| `BigQueryQueryJobMotif` | `debussy.pipeline.reverse_etl.motif.bigquery_query_job` |
| Airflow `DummyMotif` | removed (no replacement) |

Custom motifs that override `build()` and create Airflow operators keep working. New motifs can define `tasks()` instead and return engine-neutral `TaskSpec` objects (see `debussy.core.tasks`). The Airflow backend builds those as operators.
