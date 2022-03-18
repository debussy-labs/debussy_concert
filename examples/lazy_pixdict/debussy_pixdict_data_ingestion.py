#  from airflow import DAG  # noqa: F401
from airflow_concert.composition.feux_dartifice import FeuxDArtifice
from airflow.configuration import conf

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/lazy_pixdict/environment.yaml'
composition_file = f'{dags_folder}/examples/lazy_pixdict/composition.yaml'


debussy_composition: FeuxDArtifice = FeuxDArtifice.create_from_yaml(
    environment_config_yaml_filepath=env_file, composition_config_yaml_filepath=composition_file)
# rdbms_builder_fn = debussy_composition.rdbms_builder_fn()
# dags = debussy_composition.multi_play(rdbms_builder_fn)

dag = debussy_composition.auto_play()
