from debussy_concert.data_ingestion.config.bigquery_data_ingestion import ConfigBigQueryDataIngestion
from debussy_concert.data_ingestion.config.movement_parameters.bigquery import BigQueryDataIngestionMovementParameters
from debussy_concert.data_ingestion.movement.data_ingestion import DataIngestionMovement
from debussy_concert.data_ingestion.composition.base import DataIngestionBase
from debussy_concert.data_ingestion.phrase.ingestion_to_raw_vault import IngestionSourceToRawVaultStoragePhrase
from debussy_concert.data_ingestion.motif.export_table import ExportBigQueryQueryToGcsMotif


class BigQueryIngestionComposition(DataIngestionBase):
    config: ConfigBigQueryDataIngestion

    def __init__(self,):
        super().__init__()

    def auto_play(self):
        builder_fn = self.bigquery_ingestion_movement_builder
        dag = self.play(builder_fn)
        return dag

    def bigquery_ingestion_movement_builder(
            self, movement_parameters: BigQueryDataIngestionMovementParameters) -> DataIngestionMovement:
        gcs_partition = movement_parameters.data_partitioning.gcs_partition_schema
        export_motif = ExportBigQueryQueryToGcsMotif(
            gcs_partition=gcs_partition,
            extraction_query=movement_parameters.extraction_query,
            gcp_conn_id=movement_parameters.extract_connection_id)
        ingestion_to_raw_vault_phrase = IngestionSourceToRawVaultStoragePhrase(
            export_data_to_storage_motif=export_motif)
        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=ingestion_to_raw_vault_phrase
        )
