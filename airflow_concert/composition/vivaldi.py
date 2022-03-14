from airflow import DAG

from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.phrase.ingestion_to_landing import IngestionSourceToLandingStoragePhrase
from airflow_concert.phrase.landing_to_raw import LandingStorageToDataWarehouseRawPhrase
from airflow_concert.phrase.raw_to_trusted import DataWarehouseRawToTrustedPhrase
from airflow_concert.phrase.utils.start import StartPhrase
from airflow_concert.phrase.utils.end import EndPhrase
from airflow_concert.motif.export_table import ExportBigQueryTableMotif
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.services.tables.tables import TablesService
from airflow_concert.composition.composition_base import CompositionBase


class Vivaldi(CompositionBase):
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
        bq_to_gcs_ingestion = IngestionSourceToLandingStoragePhrase(export_table=ExportBigQueryTableMotif(self.config))
        phrases = [
            StartPhrase(config=config),
            bq_to_gcs_ingestion,
            LandingStorageToDataWarehouseRawPhrase(name='Landing_to_Raw_Phrase', config=config),
            DataWarehouseRawToTrustedPhrase(name='Raw_to_Trusted_Phrase', config=config),
            EndPhrase(config=config)
        ]
        name = f'Movement_{table.name}'
        four_seasons = MovementBase(name=name, config=config, phrases=phrases)
        return four_seasons
