from airflow import DAG  # noqa: F401
from airflow_concert.composer.debussy import Debussy
from airflow.configuration import conf

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/pixdict/environment.yaml'
integration_file = f'{dags_folder}/pixdict/integration.yaml'

debussy = Debussy.crate_from_yaml(environment_yaml_filepath=env_file, integration_yaml_filepath=integration_file)
mysql = debussy.mysql_composition
dag = debussy.play(mysql)
