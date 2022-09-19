import os

from airflow.configuration import conf
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.pipeline.data_ingestion.composition.rdbms_ingestion import (
    RdbmsIngestionComposition,
)
from debussy_concert.pipeline.data_ingestion.config.rdbms_data_ingestion import (
    ConfigRdbmsDataIngestion,
)

dags_folder = conf.get("core", "dags_folder")

os.environ["DEBUSSY_CONCERT__DAGS_FOLDER"] = dags_folder
os.environ["MSSQL_SAKILA_WINDOW_START"] = "execution_date.strftime('%Y-%m-%d 00:00:00')"
os.environ[
    "MSSQL_SAKILA_WINDOW_END"
] = "next_execution_date.strftime('%Y-%m-%d 00:00:00')"

env_file = f"{dags_folder}/examples/environment.yaml"
composition_file = (
    f"{dags_folder}/examples/data_ingestion/mssql_sakila_ingestion/composition.yaml"
)

workflow_service = AirflowService()
config_composition = ConfigRdbmsDataIngestion.load_from_file(
    composition_config_file_path=composition_file, env_file_path=env_file
)

inject_dependencies(workflow_service, config_composition)

debussy_composition = RdbmsIngestionComposition()
debussy_composition.dataproc_main_python_file_uri = (
    f"gs://{config_composition.environment.artifact_bucket}/pyspark-scripts"
    "/jdbc-to-gcs/jdbc_to_gcs_hash_key.py"
)

dag = debussy_composition.auto_play()
