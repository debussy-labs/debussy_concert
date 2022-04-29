from typing import Callable

from debussy_concert.data_ingestion.config.rdbms_data_ingestion import ConfigRdbmsDataIngestion
from debussy_concert.data_ingestion.config.movement_parameters.rdbms_data_ingestion import RdbmsDataIngestionMovementParameters
from debussy_concert.data_ingestion.config.movement_parameters.bigquery import BigQueryDataIngestionMovementParameters

from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.data_ingestion.movement.data_ingestion import DataIngestionMovement
from debussy_concert.core.movement.movement_base import PMovement

from debussy_concert.data_ingestion.phrase.ingestion_to_raw_vault import IngestionSourceToRawVaultStoragePhrase
from debussy_concert.data_ingestion.phrase.raw_vault_to_raw import RawVaultStorageExternalTableToDataWarehouseRawPhrase
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase

from debussy_concert.data_ingestion.motif.export_table import ExportFullMySqlTableToGcsMotif, ExportBigQueryQueryToGcsMotif
from debussy_concert.data_ingestion.motif.create_external_table import CreateExternalBigQueryTableMotif
from debussy_concert.data_ingestion.motif.merge_table import MergeBigQueryTableMotif


class FeuxDArtifice(CompositionBase):
    config: ConfigRdbmsDataIngestion

    def __init__(self,):
        super().__init__()

    def mysql_full_load_movement_builder(
            self, movement_parameters: RdbmsDataIngestionMovementParameters) -> DataIngestionMovement:
        ingestion_to_raw_vault_phrase = self.mysql_ingestion_to_raw_vault_phrase(movement_parameters)
        return self.rdbms_ingestion_movement_builder(ingestion_to_raw_vault_phrase, movement_parameters)

    def mysql_ingestion_to_raw_vault_phrase(self, movement_parameters):
        export_mysql_to_gcs_motif = ExportFullMySqlTableToGcsMotif(
            movement_parameters=movement_parameters)
        ingestion_to_raw_vault_phrase = IngestionSourceToRawVaultStoragePhrase(
            export_data_to_storage_motif=export_mysql_to_gcs_motif
        )

        return ingestion_to_raw_vault_phrase

    def auto_play(self):
        rdbms_builder_fn = self.rdbms_builder_fn()
        dag = self.play(rdbms_builder_fn)
        return dag

    def rdbms_builder_fn(self) -> Callable[[RdbmsDataIngestionMovementParameters], PMovement]:
        map_ = {
            'mysql': self.mysql_full_load_movement_builder
        }
        rdbms_name = self.config.source_type.lower()
        builder = map_.get(rdbms_name)
        if not builder:
            raise NotImplementedError(f"Invalid rdbms: {rdbms_name} not implemented")
        return builder

    def rdbms_ingestion_movement_builder(
            self, ingestion_to_raw_vault_phrase,
            movement_parameters: RdbmsDataIngestionMovementParameters) -> DataIngestionMovement:
        start_phrase = StartPhrase()
        gcs_raw_vault_to_bigquery_raw_phrase = self.gcs_raw_vault_to_bigquery_raw_phrase(movement_parameters)
        end_phrase = EndPhrase()

        name = f'DataIngestionMovement_{movement_parameters.name}'
        movement = DataIngestionMovement(
            name=name,
            start_phrase=start_phrase,
            ingestion_source_to_raw_vault_storage_phrase=ingestion_to_raw_vault_phrase,
            raw_vault_storage_to_data_warehouse_raw_phrase=gcs_raw_vault_to_bigquery_raw_phrase,
            end_phrase=end_phrase
        )
        movement.setup(movement_parameters)
        return movement

    def gcs_raw_vault_to_bigquery_raw_phrase(
            self, movement_parameters: RdbmsDataIngestionMovementParameters
    ) -> RawVaultStorageExternalTableToDataWarehouseRawPhrase:
        create_external_bigquery_table_motif = self.create_external_bigquery_table_motif()
        merge_bigquery_table_motif = self.merge_bigquery_table_motif(movement_parameters)
        gcs_raw_vault_to_bigquery_raw_phrase = RawVaultStorageExternalTableToDataWarehouseRawPhrase(
            name='RawVault_to_Raw_Phrase',
            create_external_table_motif=create_external_bigquery_table_motif,
            merge_table_motif=merge_bigquery_table_motif
        )
        return gcs_raw_vault_to_bigquery_raw_phrase

    def merge_bigquery_table_motif(
            self, movement_parameters: RdbmsDataIngestionMovementParameters) -> MergeBigQueryTableMotif:
        merge_bigquery_table_motif = MergeBigQueryTableMotif(
            movement_parameters=movement_parameters
        )
        return merge_bigquery_table_motif

    def create_external_bigquery_table_motif(
            self) -> CreateExternalBigQueryTableMotif:
        create_external_bigquery_table_motif = CreateExternalBigQueryTableMotif()
        return create_external_bigquery_table_motif

    @classmethod
    def create_from_yaml(cls, environment_config_yaml_filepath, composition_config_yaml_filepath) -> 'FeuxDArtifice':
        config = ConfigRdbmsDataIngestion.load_from_file(
            composition_config_file_path=composition_config_yaml_filepath,
            env_file_path=environment_config_yaml_filepath
        )
        return cls(config=config)
