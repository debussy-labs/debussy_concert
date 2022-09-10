import os

from airflow.configuration import conf
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.pipeline.data_ingestion.composition.bigquery_ingestion import (
    BigQueryIngestionComposition,
)
from debussy_concert.pipeline.data_ingestion.config.bigquery_data_ingestion import (
    ConfigBigQueryDataIngestion,
)

dags_folder = conf.get("core", "dags_folder")

os.environ[
    "DEBUSSY_CONCERT__DAGS_FOLDER"
] = dags_folder  # of course you can set this on your system env var

env_file = f"{dags_folder}/examples/environment.yaml"
composition_file = f"{dags_folder}/examples/data_ingestion/bigquery_ingestion_full/composition.yaml"

workflow_service = AirflowService()

config_composition = ConfigBigQueryDataIngestion.load_from_file(
    composition_config_file_path=composition_file, env_file_path=env_file
)

inject_dependencies(workflow_service, config_composition)

debussy_composition = BigQueryIngestionComposition()

dag = debussy_composition.auto_play()
