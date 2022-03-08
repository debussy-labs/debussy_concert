from airflow import DAG

from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.movement.ingestion_to_landing import IngestionToLandingMovement
from airflow_concert.movement.landing_to_raw import GcsLandingToBigQueryRawMovement
from airflow_concert.movement.raw_to_trusted import BigQueryRawToBigQueryTrustedMovement
from airflow_concert.movement.utils.start import StartMovement
from airflow_concert.movement.utils.end import EndMovement
from airflow_concert.phrase.export_table import ExportMySqlTablePhrase
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.composer.composer_base import ComposerBase
from airflow_concert.entities.table import Table


class Debussy(ComposerBase):
    def __init__(self, config: ConfigIntegration):
        super().__init__(config)

    def ingestion_composition(self, ingestion_to_landing, table: Table):
        movements = [
            StartMovement(config=self.config),
            ingestion_to_landing,
            GcsLandingToBigQueryRawMovement(name='Landing_to_Raw_Movement', config=self.config),
            BigQueryRawToBigQueryTrustedMovement(name='Raw_to_Trusted_Movement', config=self.config),
            EndMovement(config=self.config)

        ]
        name = f'Composition_{table.name}'
        ingestion = CompositionBase(name=name, config=self.config, movements=movements)
        return ingestion

    def mysql_composition(self, table: Table) -> None:
        ingestion_to_landing = IngestionToLandingMovement(ExportMySqlTablePhrase(config=self.config, table=table))
        return self.ingestion_composition(ingestion_to_landing, table)
