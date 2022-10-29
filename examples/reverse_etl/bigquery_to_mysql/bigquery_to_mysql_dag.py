from airflow.configuration import conf
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.pipeline.reverse_etl.composition.bigquery_to_mysql import BigQueryToMysql
from debussy_concert.pipeline.reverse_etl.config.reverse_etl import ConfigReverseEtl

dags_folder = conf.get("core", "dags_folder")

env_file = f"{dags_folder}/examples/environment.yaml"
composition_file = (
    f"{dags_folder}/examples/reverse_etl/bigquery_to_mysql/composition.yaml"
)

reverse_etl_config = ConfigReverseEtl.load_from_file(
    composition_config_file_path=composition_file, env_file_path=env_file
)

airflow_service = AirflowService()

inject_dependencies(
    workflow_service=airflow_service, config_composition=reverse_etl_config
)

if __name__ == "__main__":
    import pathlib

    path = pathlib.Path(__file__).parent.resolve()
    env_file = f"{path}/environment.yaml"
    composition_file = f"{path}/composition.yaml"

composition = BigQueryToMysql()
reverse_etl_movement_fn = composition.bigquery_to_storage_reverse_etl_to_rdbms_movement_builder

dag = composition.play(reverse_etl_movement_fn)
