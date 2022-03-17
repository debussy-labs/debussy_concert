#  from airflow import DAG  # noqa: F401
from airflow_concert.composition.feux_dartifice import FeuxDArtifice
from airflow.configuration import conf

dags_folder = conf.get('core', 'dags_folder')
env_file = f'{dags_folder}/examples/lazy_pixdict/environment.yaml'
integration_file = f'{dags_folder}/examples/lazy_pixdict/integration.yaml'


debussy_composition: FeuxDArtifice = FeuxDArtifice.create_from_yaml(
    environment_yaml_filepath=env_file, integration_yaml_filepath=integration_file)
rdbms_builder_fn = debussy_composition.rdbms_builder_fn()
dags = debussy_composition.multi_play(rdbms_builder_fn)
single_dag = debussy_composition.play(rdbms_builder_fn)

for dag in dags:
    globals()[dag.dag_id] = dag
