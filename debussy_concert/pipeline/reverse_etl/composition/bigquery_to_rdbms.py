from debussy_concert.pipeline.reverse_etl.composition.base import ReverseEtlBigQueryCompositionBase
from debussy_concert.pipeline.reverse_etl.movement.reverse_etl import ReverseEtlMovement
from debussy_concert.pipeline.reverse_etl.config.reverse_etl import ConfigReverseEtl
from debussy_concert.pipeline.reverse_etl.config.movement_parameters.reverse_etl import (
    ReverseEtlMovementParameters
)
from debussy_concert.pipeline.reverse_etl.phrase.storage_to_destination import (
    StorageToDestinationPhrase,
)
from debussy_concert.pipeline.reverse_etl.motif.storage_to_rdbms_motif import (
    StorageToRdbmsQueryMotif,
)
from debussy_airflow.hooks.storage_hook import GCSHook
from debussy_airflow.hooks.db_api_hook import (
    MySqlConnectorHook,
    PostgreSQLConnectorHook
)


class BigQueryToRdbms(ReverseEtlBigQueryCompositionBase):
    config: ConfigReverseEtl

    def __init__(
        self,
    ):
        super().__init__()

    def auto_play(self):
        builder_fn = self.bigquery_to_rdbms_reverse_etl_movement_builder
        dag = self.play(builder_fn)
        return dag

    def bigquery_to_rdbms_reverse_etl_movement_builder(
        self,
        movement_parameters: ReverseEtlMovementParameters,
    ) -> ReverseEtlMovement:
        return self.reverse_etl_movement_builder(
            movement_parameters=movement_parameters,
            storage_to_destination_phrases=self.storage_to_rdbms_phrase(
                movement_parameters
            )
        )

    def storage_to_rdbms_phrase(
        self, movement_parameters: ReverseEtlMovementParameters
    ):
        origin_gcs_hook = GCSHook(
            gcp_conn_id=self.config.environment.data_lakehouse_connection_id
        )

        destination_type = movement_parameters.destinations[0]["type"].lower()
        destination_type_motif_map = {
            "mysql": MySqlConnectorHook,
            "pgsql": PostgreSQLConnectorHook,
        }
        HookCls = destination_type_motif_map[destination_type]
        destination_dbapi_hook = HookCls(
            movement_parameters.destinations[0]["connection_id"]
        )

        storage_to_rdbms_destination = StorageToRdbmsQueryMotif(
            name=f"storage_to_{destination_type}_motif",
            dbapi_hook=destination_dbapi_hook,
            storage_hook=origin_gcs_hook,
            destination_table=movement_parameters.destinations[0]["uri"],
        )

        phrase = StorageToDestinationPhrase(
            name="StorageToRdbmsDestinationPhrase",
            storage_to_destination_motif=storage_to_rdbms_destination,
        )
        return phrase
