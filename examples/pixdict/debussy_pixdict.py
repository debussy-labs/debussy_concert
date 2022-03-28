import inject
from debussy_concert.data_ingestion.composition.debussy import Debussy
from airflow.configuration import conf

from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.core.service.workflow.protocol import PWorkflowService


def config_services(binder: inject.Binder):
    binder.bind(PWorkflowService, AirflowService())


inject.configure(config_services, bind_in_runtime=False)


dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/pixdict/environment.yaml'
composition_file = f'{dags_folder}/examples/pixdict/composition.yaml'


debussy: Debussy = Debussy.create_from_yaml(
    environment_config_yaml_filepath=env_file, composition_config_yaml_filepath=composition_file)
mysql = debussy.mysql_movement_builder
dags = debussy.multi_play(mysql)

for dag in dags:
    globals()[dag.dag_id] = dag
