## Changelog
- 0.1.0 - First release
- 1.0.0 - Improved folder structure and dependencies
- 1.1.0 - Add support for Dataproc Serverless and quick fix for BigQuery load with schema
- 1.2.0 - Fix packaging (1.0.0 and 1.1.0 shipped without subpackages), require debussy-airflow 0.0.7+ (installable again), add package summary, add airflow_easy_setup and Dataproc Serverless support
- 2.0.0 (unreleased) - Package renamed from `debussy_concert` to `debussy` (import `debussy`, environment variable prefix `DEBUSSY__`). Requires Python 3.11+ and Airflow 3.4+; the debussy-airflow and airflow-dbt dependencies are replaced by operators and hooks in debussy itself. DAG parameter `schedule_interval` is now `schedule`, and templates use `logical_date` and `data_interval_end`
