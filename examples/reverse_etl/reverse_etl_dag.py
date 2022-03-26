import inject
from airflow.configuration import conf
from debussy_concert.reverse_etl.composition.childrens_corner import ChildrensCorner
from debussy_concert.service.workflow.airflow import AirflowService
from debussy_concert.service.workflow.protocol import PWorkflowService


def config_services(binder: inject.Binder):
    airflow_service = AirflowService()
    binder.bind(PWorkflowService, airflow_service)


inject.configure(config_services, bind_in_runtime=False)

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/reverse_etl/environment.yaml'
composition_file = f'{dags_folder}/examples/reverse_etl/composition.yaml'
if __name__ == '__main__':
    import pathlib
    path = pathlib.Path(__file__).parent.resolve()
    env_file = f'{path}/environment.yaml'
    composition_file = f'{path}/composition.yaml'


composition: ChildrensCorner = ChildrensCorner.create_from_yaml(
    environment_config_yaml_filepath=env_file, composition_config_yaml_filepath=composition_file)
reverse_etl_movement_fn = composition.gcs_reverse_etl_movement_builder
dag = composition.play(reverse_etl_movement_fn)
