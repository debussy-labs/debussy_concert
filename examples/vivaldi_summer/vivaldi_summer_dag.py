from airflow import DAG
from airflow_concert.compositor.vivaldi import Vivaldi
from airflow.configuration import conf

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/vivaldi_summer/environment.yaml'
integration_file = f'{dags_folder}/vivaldi_summer/integration.yaml'

summer = Vivaldi.summer(environment_yaml_filepath=env_file, integration_yaml_filepath=integration_file)
# summer.show()
dag = summer.build()
