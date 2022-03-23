import inject
from airflow.configuration import conf
from debussy_concert.composition.feux_dartifice import FeuxDArtifice
from debussy_concert.service.workflow.airflow import AirflowService
from debussy_concert.service.workflow.protocol import PWorkflowService


def config_services(binder: inject.Binder):
    binder.bind(PWorkflowService, AirflowService())


inject.configure(config_services, bind_in_runtime=False)

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/lazy_pixdict/environment.yaml'
composition_file = f'{dags_folder}/examples/lazy_pixdict/composition.yaml'


debussy_composition: FeuxDArtifice = FeuxDArtifice.create_from_yaml(
    environment_config_yaml_filepath=env_file, composition_config_yaml_filepath=composition_file)
# rdbms_builder_fn = debussy_composition.rdbms_builder_fn()
# dags = debussy_composition.multi_play(rdbms_builder_fn)

dag = debussy_composition.auto_play()
