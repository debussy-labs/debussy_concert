from debussy_concert.data_ingestion.config.bigquery_data_ingestion import ConfigBigQueryDataIngestion
from debussy_concert.data_ingestion.config.movement_parameters.bigquery import BigQueryDataIngestionMovementParameters

from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.data_ingestion.movement.data_ingestion import DataIngestionMovement


from debussy_concert.data_ingestion.phrase.ingestion_to_raw_vault import IngestionSourceToRawVaultStoragePhrase
from debussy_concert.data_ingestion.phrase.raw_vault_to_raw import RawVaultStorageLoadToDataWarehouseRawPhrase
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase
from debussy_concert.core.motif.mixins.bigquery_job import BigQueryTimePartitioning, HivePartitioningOptions
from debussy_concert.data_ingestion.motif.export_table import ExportBigQueryQueryToGcsMotif
from debussy_concert.data_ingestion.motif.load_table_motif import LoadGcsToBigQueryHivePartitionMotif


class BigQueryIngestionComposition(CompositionBase):
    config: ConfigBigQueryDataIngestion

    def __init__(self,):
        super().__init__()

    def auto_play(self):
        builder_fn = self.bigquery_ingestion_movement_builder
        dag = self.play(builder_fn)
        return dag

    def bigquery_ingestion_movement_builder(
            self, movement_parameters: BigQueryDataIngestionMovementParameters) -> DataIngestionMovement:
        start_phrase = StartPhrase()
        gcs_partition = movement_parameters.data_partitioning.gcs_partition_schema
        export_motif = ExportBigQueryQueryToGcsMotif(
            gcs_partition=gcs_partition,
            extract_query=movement_parameters.extract_sql_query,
            gcp_conn_id=movement_parameters.extract_connection_id)
        ingestion_to_raw_vault_phrase = IngestionSourceToRawVaultStoragePhrase(
            export_data_to_storage_motif=export_motif)
        gcs_raw_vault_to_bigquery_raw_phrase = self.gcs_raw_vault_to_bigquery_raw_phrase(
            movement_parameters, gcs_partition)
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
        self, movement_parameters: BigQueryDataIngestionMovementParameters,
        gcs_partition
    ):
        source_uri_prefix = (f"gs://{self.config.environment.raw_vault_bucket}/"
                             f"{self.config.source_type}/{self.config.source_name}/{movement_parameters.name}")


        load_bigquery_from_gcs = self.load_bigquery_from_gcs_hive_partition_motif(
            gcp_conn_id=movement_parameters.extract_connection_id,
            source_uri_prefix=source_uri_prefix,
            gcs_partition=gcs_partition,
            partition_granularity=movement_parameters.data_partitioning.partition_granularity,
            partition_field=movement_parameters.data_partitioning.partition_field,
            destination_partition=movement_parameters.data_partitioning.destination_partition
        )

        
        gcs_raw_vault_to_bigquery_raw_phrase = RawVaultStorageLoadToDataWarehouseRawPhrase(
            load_table_from_storage_motif=load_bigquery_from_gcs
        )
        return gcs_raw_vault_to_bigquery_raw_phrase

    def load_bigquery_from_gcs_hive_partition_motif(
            self, gcp_conn_id, source_uri_prefix,
            gcs_partition, partition_granularity,
            partition_field, destination_partition):
        hive_options = HivePartitioningOptions()
        hive_options.mode = "AUTO"
        hive_options.source_uri_prefix = source_uri_prefix
        time_partitioning = BigQueryTimePartitioning(type=partition_granularity, field=partition_field)
        motif = LoadGcsToBigQueryHivePartitionMotif(
            name='load_bigquery_from_gcs_hive_partition_motif',
            gcs_partition=gcs_partition,
            destination_partition=destination_partition,
            source_format='PARQUET',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            hive_partitioning_options=hive_options,
            time_partitioning=time_partitioning,
            schema_update_options=None,
            gcp_conn_id=gcp_conn_id
        )
        return motif
