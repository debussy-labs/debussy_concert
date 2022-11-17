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
from debussy_airflow.hooks.db_api_hook import MySqlConnectorHook


class BigQueryToMysql(ReverseEtlBigQueryCompositionBase):
    config: ConfigReverseEtl

    def __init__(
        self,
    ):
        super().__init__()

    def auto_play(self):
        builder_fn = self.bigquery_to_storage_reverse_etl_to_rdbms_movement_builder
        dag = self.play(builder_fn)
        return dag

    def bigquery_to_storage_reverse_etl_to_rdbms_movement_builder(
        self,
        movement_parameters: ReverseEtlMovementParameters,
    ) -> ReverseEtlMovement:
        return self.reverse_etl_movement_builder(
            movement_parameters=movement_parameters,
            storage_to_destination_phrases=[
                self.storage_to_mysql_phrase(
                    movement_parameters
                )
            ]
        )

    def storage_to_mysql_phrase(
        self, movement_parameters: ReverseEtlMovementParameters
    ):
        storage_hook = GCSHook(
            gcp_conn_id=self.config.environment.data_lakehouse_connection_id
        )
        dbapi_hook = MySqlConnectorHook(
            rdbms_conn_id=movement_parameters.destinations[0]["connection_id"]
        )
        storage_to_mysql_destination = StorageToRdbmsQueryMotif(
            name="storage_to_mysql_motif",
            dbapi_hook=dbapi_hook,
            storage_hook=storage_hook,
            destination_table=movement_parameters.destinations[0]["uri"],
        )
        phrase = StorageToDestinationPhrase(
            name="StorageToRdbmsDestinationPhrase",
            storage_to_destination_motif=storage_to_mysql_destination,
        )
        return phrase
