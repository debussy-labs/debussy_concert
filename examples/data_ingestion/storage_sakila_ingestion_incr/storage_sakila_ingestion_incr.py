import os

from airflow.configuration import conf
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.pipeline.data_ingestion.config.storage_data_ingestion import (
    ConfigStorageDataIngestion,
)
from debussy_concert.pipeline.data_ingestion.composition.storage_ingestion import (
    StorageIngestionComposition,
)

dags_folder = conf.get("core", "dags_folder")

os.environ["DEBUSSY_CONCERT__DAGS_FOLDER"] = dags_folder
os.environ["STORAGE_INGESTION_WINDOW_START"] = "execution_date.strftime('%Y-%m-%d 00:00:00')"
os.environ["STORAGE_INGESTION_WINDOW_END"] = "next_execution_date.strftime('%Y-%m-%d 00:00:00')"

env_file = f"{dags_folder}/examples/environment.yaml"
composition_file = f"{dags_folder}/examples/data_ingestion/storage_sakila_ingestion_incr/composition.yaml"

workflow_service = AirflowService()
config_composition = ConfigStorageDataIngestion.load_from_file(
    composition_config_file_path=composition_file, env_file_path=env_file
)

inject_dependencies(workflow_service, config_composition)

debussy_composition = StorageIngestionComposition()

dag = debussy_composition.auto_play()
