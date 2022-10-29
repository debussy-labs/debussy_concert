from debussy_concert.pipeline.reverse_etl.composition.base import ReverseEtlBigQueryCompositionBase
from debussy_concert.pipeline.reverse_etl.movement.reverse_etl import ReverseEtlMovement
from debussy_concert.pipeline.reverse_etl.config.reverse_etl import ConfigReverseEtl
from debussy_concert.pipeline.reverse_etl.config.movement_parameters.reverse_etl import (
    ReverseEtlMovementParameters
)
from debussy_concert.pipeline.reverse_etl.phrase.storage_to_destination import (
    StorageToDestinationPhrase,
)
from debussy_concert.pipeline.reverse_etl.motif.storage_to_storage_motif import (
    StorageToStorageMotif,
)
from debussy_airflow.hooks.storage_hook import GCSHook, SFTPHook


class ReverseEtlBigQueryToStorageComposition(ReverseEtlBigQueryCompositionBase):
    config: ConfigReverseEtl

    def __init__(
        self,
    ):
        super().__init__()

    def auto_play(self):
        builder_fn = self.bigquery_to_storage_reverse_etl_movement_builder
        dag = self.play(builder_fn)
        return dag

    def bigquery_to_storage_reverse_etl_movement_builder(
        self,
        movement_parameters: ReverseEtlMovementParameters,
    ) -> ReverseEtlMovement:
        return self.reverse_etl_movement_builder(
            movement_parameters=movement_parameters,
            storage_to_destination_phrases=[
                self.storage_to_storage_phrase(
                    movement_parameters
                )
            ]
        )

    def storage_to_storage_phrase(
        self, movement_parameters: ReverseEtlMovementParameters
    ):
        destination_file_uri = movement_parameters.destinations[0]["uri"]
        destination_type = movement_parameters.destinations[0]["type"].lower()
        destination_type_motif_map = {
            "gcs": GCSHook,
            "sftp": SFTPHook,
        }
        HookCls = destination_type_motif_map[destination_type]
        origin_gcs_hook = GCSHook(
            gcp_conn_id=self.config.environment.data_lakehouse_connection_id
        )
        destination_hook = HookCls(
            movement_parameters.destinations[0]["connection_id"]
        )
        storage_to_storage_motif = StorageToStorageMotif(
            name=f"gcs_to_{destination_type}_motif",
            origin_storage_hook=origin_gcs_hook,
            destiny_storage_hook=destination_hook,
            destiny_file_uri=destination_file_uri,
        )
        phrase = StorageToDestinationPhrase(
            storage_to_destination_motif=storage_to_storage_motif
        )
        return phrase
