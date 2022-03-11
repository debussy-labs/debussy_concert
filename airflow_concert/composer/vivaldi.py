from airflow import DAG

from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.movement.ingestion_to_landing import IngestionToLandingMovement
from airflow_concert.movement.landing_to_raw import GcsLandingToBigQueryRawMovement
from airflow_concert.movement.raw_to_trusted import BigQueryRawToBigQueryTrustedMovement
from airflow_concert.movement.utils.start import StartMovement
from airflow_concert.movement.utils.end import EndMovement
from airflow_concert.phrase.export_table import ExportBigQueryTablePhrase
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.services.tables.tables import TablesService
from airflow_concert.composer.composer_base import ComposerBase


class Vivaldi(ComposerBase):
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

    def four_seasons(self, table) -> None:
        config = self.config
        bq_to_gcs_ingestion = IngestionToLandingMovement(export_table=ExportBigQueryTablePhrase(self.config))
        movements = [
            StartMovement(config=config),
            bq_to_gcs_ingestion,
            GcsLandingToBigQueryRawMovement(name='Landing_to_Raw_Movement', config=config),
            BigQueryRawToBigQueryTrustedMovement(name='Raw_to_Trusted_Movement', config=config),
            EndMovement(config=config)
        ]
        name = f'Composition_{table.name}'
        four_seasons = CompositionBase(name=name, config=config, movements=movements)
        return four_seasons
