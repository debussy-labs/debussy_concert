from typing import Callable

from debussy_concert.config.data_ingestion import ConfigDataIngestion
from debussy_concert.config.movement_parameters.data_ingestion import DataIngestionMovementParameters

from debussy_concert.composition.composition_base import CompositionBase
from debussy_concert.movement.data_ingestion import DataIngestionMovement
from debussy_concert.movement.movement_base import PMovement

from debussy_concert.phrase.ingestion_to_landing import IngestionSourceToLandingStoragePhrase
from debussy_concert.phrase.landing_to_raw import LandingStorageExternalTableToDataWarehouseRawPhrase
from debussy_concert.phrase.raw_to_trusted import DataWarehouseRawToTrustedPhrase
from debussy_concert.phrase.utils.start import StartPhrase
from debussy_concert.phrase.utils.end import EndPhrase

from debussy_concert.motif.export_table import ExportFullMySqlTableToGcsMotif
from debussy_concert.motif.bigquery_query_job import BigQueryQueryJobMotif
from debussy_concert.motif.create_external_table import CreateExternalBigQueryTableMotif
from debussy_concert.motif.merge_table import MergeBigQueryTableMotif


class FeuxDArtifice(CompositionBase):
    def __init__(self, config: ConfigDataIngestion):
        super().__init__(config=config)

    def mysql_full_load_movement_builder(
            self, movement_parameters: DataIngestionMovementParameters) -> DataIngestionMovement:
        ingestion_to_landing_phrase = self.mysql_ingestion_to_landing_phrase(movement_parameters)
        return self.rdbms_ingestion_movement_builder(ingestion_to_landing_phrase, movement_parameters)

    def mysql_ingestion_to_landing_phrase(self, movement_parameters):
        export_mysql_to_gcs_motif = ExportFullMySqlTableToGcsMotif(
            config=self.config, movement_parameters=movement_parameters)
        ingestion_to_landing_phrase = IngestionSourceToLandingStoragePhrase(
            export_data_to_storage_motif=export_mysql_to_gcs_motif
        )

        return ingestion_to_landing_phrase

    def auto_play(self):
        rdbms_builder_fn = self.rdbms_builder_fn()
        dag = self.play(rdbms_builder_fn)
        return dag

    def rdbms_builder_fn(self) -> Callable[[DataIngestionMovementParameters], PMovement]:
        map_ = {
            'mysql': self.mysql_full_load_movement_builder
        }
        rdbms_name = self.config.rdbms_name
        builder = map_.get(rdbms_name)
        if not builder:
            raise NotImplementedError(f"Invalid rdbms: {rdbms_name} not implemented")
        return builder

    def rdbms_ingestion_movement_builder(
            self, ingestion_to_landing_phrase,
            movement_parameters: DataIngestionMovementParameters) -> DataIngestionMovement:
        start_phrase = StartPhrase(config=self.config)
        gcs_landing_to_bigquery_raw_phrase = self.gcs_landing_to_bigquery_raw_phrase(movement_parameters)
        data_warehouse_raw_to_trusted_phrase = self.data_warehouse_raw_to_trusted_phrase()
        end_phrase = EndPhrase(config=self.config)

        name = f'DataIngestionMovement_{movement_parameters.name}'
        movement = DataIngestionMovement(
            name=name,
            start_phrase=start_phrase,
            ingestion_source_to_landing_storage_phrase=ingestion_to_landing_phrase,
            landing_storage_to_data_warehouse_raw_phrase=gcs_landing_to_bigquery_raw_phrase,
            data_warehouse_raw_to_trusted_phrase=data_warehouse_raw_to_trusted_phrase,
            end_phrase=end_phrase
        )
        movement.setup(self.config, movement_parameters)
        return movement

    def data_warehouse_raw_to_trusted_phrase(self) -> DataWarehouseRawToTrustedPhrase:
        bigquery_to_bigquery_motif = BigQueryQueryJobMotif().setup(sql_query='select 1')
        data_warehouse_raw_to_trusted_phrase = DataWarehouseRawToTrustedPhrase(
            name='Raw_to_Trusted_Phrase',
            raw_to_trusted_motif=bigquery_to_bigquery_motif
        )
        return data_warehouse_raw_to_trusted_phrase

    def gcs_landing_to_bigquery_raw_phrase(
            self, movement_parameters: DataIngestionMovementParameters
    ) -> LandingStorageExternalTableToDataWarehouseRawPhrase:
        create_external_bigquery_table_motif = self.create_external_bigquery_table_motif()
        merge_bigquery_table_motif = self.merge_bigquery_table_motif(movement_parameters)
        gcs_landing_to_bigquery_raw_phrase = LandingStorageExternalTableToDataWarehouseRawPhrase(
            name='Landing_to_Raw_Phrase',
            create_external_table_motif=create_external_bigquery_table_motif,
            merge_table_motif=merge_bigquery_table_motif
        )
        return gcs_landing_to_bigquery_raw_phrase

    def execute_query_motif(self, sql_query):
        execute_query_motif = BigQueryQueryJobMotif().setup(sql_query=sql_query)
        return execute_query_motif

    def merge_bigquery_table_motif(
            self, movement_parameters: DataIngestionMovementParameters) -> MergeBigQueryTableMotif:
        merge_bigquery_table_motif = MergeBigQueryTableMotif(
            config=self.config,
            movement_parameters=movement_parameters
        )
        return merge_bigquery_table_motif

    def create_external_bigquery_table_motif(
            self) -> CreateExternalBigQueryTableMotif:
        create_external_bigquery_table_motif = CreateExternalBigQueryTableMotif(
            config=self.config
        )
        return create_external_bigquery_table_motif

    @classmethod
    def create_from_yaml(cls, environment_config_yaml_filepath, composition_config_yaml_filepath) -> 'FeuxDArtifice':
        config = ConfigDataIngestion.load_from_file(
            composition_config_file_path=composition_config_yaml_filepath,
            env_file_path=environment_config_yaml_filepath
        )
        return cls(config)
