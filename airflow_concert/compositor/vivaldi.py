from airflow import DAG

from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.movement.ingestion_to_landing.bigquery_to_gcs import BigQueryToGcsMovement
from airflow_concert.movement.landing_to_raw.gcs_to_bigquery import GcsToBigQueryMovement
from airflow_concert.movement.raw_to_trusted.bigquery_to_bigquery import BigQueryToBigQueryMovement
from airflow_concert.movement.utils.start import StartMovement
from airflow_concert.movement.utils.end import EndMovement
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.services.tables import TablesService


class Vivaldi:
    def __init__(self, config: ConfigIntegration):
        self.config = config
        self.tables_service = TablesService.create_from_dict(config.tables)

    @classmethod
    def crate_from_yaml(cls, environment_yaml_filepath, integration_yaml_filepath) -> 'Vivaldi':
        config = ConfigIntegration.load_from_file(
            integration_file_path=integration_yaml_filepath,
            env_file_path=environment_yaml_filepath
        )
        return cls(config)

    def summer(self) -> None:
        config = self.config
        movements = [
            StartMovement(config=config),
            BigQueryToGcsMovement(name='Ingestion_to_Landing_Movement', config=config),
            GcsToBigQueryMovement(name='Landing_to_Raw_Movement', config=config),
            BigQueryToBigQueryMovement(name='Raw_to_Trusted_Movement', config=config),
            EndMovement(config=config)]
        summer = CompositionBase(config=config, movements=movements)
        return summer

    def build(self, composition: CompositionBase) -> DAG:
        dag = DAG(**self.config.dag_parameters)
        for table in self.tables_service.tables():
            composition.name = table.name
            composition.build(dag=dag)
        return dag
