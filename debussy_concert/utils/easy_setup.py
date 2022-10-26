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


def setup_this_composition_for_airflow(rel_path_composition_file, rel_path_env_file, composition_type):

    dags_folder = conf.get("core", "dags_folder")
    composition_file = f"{dags_folder}/{rel_path_composition_file}"
    env_file = f"{dags_folder}/{rel_path_env_file}"

    os.environ[
        "DEBUSSY_CONCERT__DAGS_FOLDER"
    ] = dags_folder

    _ingestion_type_map = {
        'ingestion_bigquery': {
            'config': ConfigBigQueryDataIngestion,
            'composition': BigQueryIngestionComposition
        }
    }

    workflow_service = AirflowService()
    config_composition = _ingestion_type_map[composition_type]['config'].load_from_file(
        composition_config_file_path=composition_file, env_file_path=env_file
    )

    inject_dependencies(workflow_service, config_composition)

    debussy_composition = _ingestion_type_map[composition_type]['composition']()
    return debussy_composition
