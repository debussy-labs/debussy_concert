import inject
from airflow.configuration import conf

from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.data_ingestion.config.data_ingestion import ConfigDataIngestion
from debussy_concert.data_ingestion.composition.feux_dartifice import FeuxDArtifice
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.core.service.workflow.protocol import PWorkflowService


dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/lazy_pixdict/environment.yaml'
composition_file = f'{dags_folder}/examples/lazy_pixdict/composition.yaml'
workflow_service = AirflowService()
config_composition = ConfigDataIngestion.load_from_file(
    composition_config_file_path=composition_file,
    env_file_path=env_file)


def config_services(binder: inject.Binder):
    binder.bind(PWorkflowService, workflow_service)
    binder.bind(ConfigComposition, config_composition)


inject.configure(config_services, bind_in_runtime=False)

debussy_composition = FeuxDArtifice()
# rdbms_builder_fn = debussy_composition.rdbms_builder_fn()
# dags = debussy_composition.multi_play(rdbms_builder_fn)

dag = debussy_composition.auto_play()
