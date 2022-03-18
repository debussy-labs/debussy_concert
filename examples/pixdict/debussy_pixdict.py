#  from airflow import DAG  # noqa: F401
from airflow_concert.composition.debussy import Debussy
from airflow.configuration import conf

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/pixdict/environment.yaml'
composition_file = f'{dags_folder}/examples/pixdict/composition.yaml'


debussy: Debussy = Debussy.create_from_yaml(
    environment_config_yaml_filepath=env_file, composition_config_yaml_filepath=composition_file)
mysql = debussy.mysql_movement_builder
dags = debussy.multi_play(mysql)

for dag in dags:
    globals()[dag.dag_id] = dag
