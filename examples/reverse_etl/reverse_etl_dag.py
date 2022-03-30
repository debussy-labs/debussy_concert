import inject
from airflow.configuration import conf
from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.reverse_etl.config.reverse_etl import ConfigReverseEtl
from debussy_concert.reverse_etl.composition.childrens_corner import ChildrensCorner
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.core.service.workflow.protocol import PWorkflowService

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/reverse_etl/environment.yaml'
composition_file = f'{dags_folder}/examples/reverse_etl/composition.yaml'

composition_config = ConfigReverseEtl.load_from_file(
    composition_config_file_path=composition_file,
    env_file_path=env_file
)
airflow_service = AirflowService()


def config_services(binder: inject.Binder):
    binder.bind(PWorkflowService, airflow_service)
    binder.bind(ConfigComposition, composition_config)


inject.configure(config_services, bind_in_runtime=False)

if __name__ == '__main__':
    import pathlib
    path = pathlib.Path(__file__).parent.resolve()
    env_file = f'{path}/environment.yaml'
    composition_file = f'{path}/composition.yaml'


composition: ChildrensCorner = ChildrensCorner()
reverse_etl_movement_fn = composition.gcs_reverse_etl_movement_builder
dag = composition.play(reverse_etl_movement_fn)
