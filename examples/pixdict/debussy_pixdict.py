import inject
from debussy_concert.data_ingestion.composition.debussy import Debussy
from airflow.configuration import conf

from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.data_ingestion.config.data_ingestion import ConfigDataIngestion
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.core.service.workflow.protocol import PWorkflowService


dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/pixdict/environment.yaml'
composition_file = f'{dags_folder}/examples/pixdict/composition.yaml'
workflow_service = AirflowService()
config_composition = ConfigDataIngestion.load_from_file(
    composition_config_file_path=composition_file,
    env_file_path=env_file)


def config_services(binder: inject.Binder):
    binder.bind(PWorkflowService, workflow_service)
    binder.bind(ConfigComposition, config_composition)


inject.configure(config_services, bind_in_runtime=False)

debussy = Debussy()
mysql = debussy.mysql_movement_builder
dags = debussy.multi_play(mysql)

for dag in dags:
    globals()[dag.dag_id] = dag
