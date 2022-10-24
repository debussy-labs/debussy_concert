from debussy_concert.pipeline.data_ingestion.composition.base import DataIngestionBase
from debussy_concert.pipeline.data_ingestion.movement.data_ingestion import DataIngestionMovement
from debussy_concert.pipeline.data_ingestion.config.bigquery_data_ingestion import ConfigBigQueryDataIngestion
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.gcs import GcsMovementParameters

from debussy_concert.pipeline.reverse_etl.phrase.storage_to_destination import (
    StorageToDestinationPhrase,
)

from debussy_concert.pipeline.reverse_etl.motif.storage_to_storage_motif import (
    StorageToStorageMotif,
)

from debussy_airflow.hooks.storage_hook import GCSHook, SFTPHook


class GcsIngestionComposition(DataIngestionBase):
    config: ConfigBigQueryDataIngestion

    def __init__(
        self,
    ):
        super().__init__()

    def auto_play(self):
        builder_fn = self.storage_ingestion_movement_builder
        dag = self.play(builder_fn)
        return dag

    def storage_ingestion_movement_builder(
        self, movement_parameters: GcsMovementParameters
    ) -> DataIngestionMovement:

        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.storage_to_destination_phrase(
                movement_parameters
            ),
        )

    def storage_to_destination_phrase(
        self, movement_parameters: GcsMovementParameters
    ):
        destination_file_uri = movement_parameters.destination_uri
        destination_type = movement_parameters.destination_type.lower()
        destination_type_motif_map = {
            "gcs": GCSHook,
            "sftp": SFTPHook,
        }
        HookCls = destination_type_motif_map[destination_type]
        destiny_storage_hook = GCSHook(
            gcp_conn_id=self.config.environment.data_lakehouse_connection_id
        )
        origin_storage_hook = HookCls(
            movement_parameters.destination_connection_id)
        storage_to_storage_motif = StorageToStorageMotif(
            name=f"gcs_to_{destination_type}_motif",
            origin_storage_hook=origin_storage_hook,
            destiny_storage_hook=destiny_storage_hook,
            destiny_file_uri=destination_file_uri,
        )
        phrase = StorageToDestinationPhrase(
            storage_to_destination_motif=storage_to_storage_motif
        )
        return phrase
