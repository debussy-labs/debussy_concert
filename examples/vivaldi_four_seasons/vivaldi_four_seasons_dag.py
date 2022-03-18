#  from airflow import DAG  # noqa: F401
#  from airflow_concert.composition.vivaldi import Vivaldi
from airflow.configuration import conf

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/vivaldi_four_seasons/environment.yaml'
composition_file = f'{dags_folder}/examples/vivaldi_four_seasons/composition.yaml'
