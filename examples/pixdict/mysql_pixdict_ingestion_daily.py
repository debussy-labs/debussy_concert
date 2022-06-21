from airflow.configuration import conf

from debussy_concert.data_ingestion.config.rdbms_data_ingestion import ConfigRdbmsDataIngestion
from debussy_concert.data_ingestion.composition.rdbms_ingestion import RdbmsIngestionComposition
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService


dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/pixdict/environment.yaml'
composition_file = f'{dags_folder}/examples/pixdict/composition_daily.yaml'
workflow_service = AirflowService()
config_composition = ConfigRdbmsDataIngestion.load_from_file(
    composition_config_file_path=composition_file,
    env_file_path=env_file)

inject_dependencies(workflow_service, config_composition)

debussy_composition = RdbmsIngestionComposition()

dag = debussy_composition.auto_play()
