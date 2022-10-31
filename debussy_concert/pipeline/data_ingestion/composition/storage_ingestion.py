from debussy_concert.pipeline.data_ingestion.composition.base import DataIngestionBase
from debussy_concert.pipeline.data_ingestion.movement.data_ingestion import DataIngestionMovement
from debussy_concert.pipeline.data_ingestion.phrase.ingestion_to_raw_vault import \
    IngestionSourceToRawVaultStoragePhrase
from debussy_concert.pipeline.data_ingestion.config.storage_data_ingestion import ConfigStorageDataIngestion
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.storage_data_ingestion import \
    StorageDataIngestionMovementParameters
from debussy_concert.pipeline.data_ingestion.motif.storage_to_storage import (
    StorageToRawVaultMotif,
    csv_to_parquet,
    parquet_to_parquet
)
from debussy_airflow.hooks.storage_hook import GCSHook, SFTPHook, S3Hook


class StorageIngestionComposition(DataIngestionBase):
    config: ConfigStorageDataIngestion

    def __init__(
        self,
    ):
        super().__init__()

    def auto_play(self):
        builder_fn = self.storage_ingestion_movement_builder
        dag = self.play(builder_fn)
        return dag

    def storage_ingestion_movement_builder(
        self, movement_parameters: StorageDataIngestionMovementParameters
    ) -> DataIngestionMovement:

        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.storage_to_raw_vault_phrase(
                movement_parameters
            ),
        )

    def storage_to_raw_vault_phrase(
        self, movement_parameters: StorageDataIngestionMovementParameters
    ):
        source_type = movement_parameters.source_config.type.lower()
        source_type_hook_map = {
            "gcs": GCSHook,
            "sftp": SFTPHook,
            "s3": S3Hook
        }
        HookCls = source_type_hook_map[source_type]
        source_storage_hook = HookCls(
            movement_parameters.source_config.connection_id
        )

        raw_vault_hook = GCSHook(
            gcp_conn_id=self.config.environment.data_lakehouse_connection_id
        )

        source_format = movement_parameters.source_config.format.lower()
        file_transformer_fn_map = {
            "csv": csv_to_parquet,
            "parquet": parquet_to_parquet
        }
        file_transformer_fn = file_transformer_fn_map[source_format]

        storage_to_raw_vault_motif = StorageToRawVaultMotif(
            name="storage_to_raw_vault_motif",
            source_storage_hook=source_storage_hook,
            raw_vault_hook=raw_vault_hook,
            source_file_uri=movement_parameters.source_config.uri,
            is_dir=movement_parameters.source_config.is_dir,
            file_transformer_callable=file_transformer_fn,
            gcs_partition=movement_parameters.data_partitioning.gcs_partition_schema,
        )

        storage_to_raw_vault_phrase = IngestionSourceToRawVaultStoragePhrase(
            export_data_to_storage_motif=storage_to_raw_vault_motif
        )

        return storage_to_raw_vault_phrase
