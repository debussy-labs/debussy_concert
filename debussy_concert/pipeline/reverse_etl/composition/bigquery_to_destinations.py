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
from debussy_concert.pipeline.reverse_etl.motif.storage_to_rdbms_motif import (
    StorageToRdbmsQueryMotif,
)
from debussy_airflow.hooks.storage_hook import GCSHook, SFTPHook
from debussy_airflow.hooks.db_api_hook import MySqlConnectorHook, PostgreSQLConnectorHook


class BigQueryToDestinationsComposition(ReverseEtlBigQueryCompositionBase):
    config: ConfigReverseEtl

    def __init__(
        self,
    ):
        super().__init__()

    def auto_play(self):
        builder_fn = self.bigquery_to_destinations_reverse_etl_movement_builder
        dag = self.play(builder_fn)
        return dag

    def bigquery_to_destinations_reverse_etl_movement_builder(
        self,
        movement_parameters: ReverseEtlMovementParameters,
    ) -> ReverseEtlMovement:
        return self.reverse_etl_movement_builder(
            movement_parameters=movement_parameters,
            storage_to_destination_phrases=self.reverse_etl_storage_to_destination_phrases(
                movement_parameters
            )
        )

    def reverse_etl_storage_to_destination_phrases(
        self, movement_parameters: ReverseEtlMovementParameters
    ):
        """
        Constructs the reverse ETL phrases that moves data from the reverse_etl storage to each destination.
        """
        destination_phrases = []
        for destination in movement_parameters.destinations:
            phrase = self.get_phrase_from_destination(destination)
            destination_phrases.append(phrase)
        return destination_phrases

    def get_phrase_from_destination(self, destination):
        origin_gcs_hook = GCSHook(
            gcp_conn_id=self.config.environment.data_lakehouse_connection_id
        )
        destination_name = destination["name"]
        destination_type = destination["type"].lower()
        destination_type_motif_map = {
            "gcs": self.storage_to_gcs_motif,
            "sftp": self.storage_to_sftp_motif,
            "mysql": self.storage_to_mysql_motif,
            "postgresql": self.storage_to_postgresql_motif,
        }
        storage_to_destination_fn = destination_type_motif_map[destination_type]

        phrase = StorageToDestinationPhrase(
            name=f"StorageToDestinationPhrase_{destination_name}",
            storage_to_destination_motif=storage_to_destination_fn(
                origin_gcs_hook, destination)
        )

        return phrase

    def storage_to_gcs_motif(self, origin_gcs_hook, destination):
        return StorageToStorageMotif(
            name="storage_to_gcs_motif",
            origin_storage_hook=origin_gcs_hook,
            destiny_storage_hook=GCSHook,
            destiny_file_uri=destination["uri"]
        )

    def storage_to_sftp_motif(self, origin_gcs_hook, destination):
        return StorageToStorageMotif(
            name="storage_to_sftp_motif",
            origin_storage_hook=origin_gcs_hook,
            destiny_storage_hook=SFTPHook,
            destiny_file_uri=destination["uri"]
        )

    def storage_to_mysql_motif(self, origin_gcs_hook, destination):
        return StorageToRdbmsQueryMotif(
            name="storage_to_mysql_motif",
            dbapi_hook=MySqlConnectorHook,
            storage_hook=origin_gcs_hook,
            destination_table=destination["uri"]
        )

    def storage_to_postgresql_motif(self, origin_gcs_hook, destination):
        return StorageToRdbmsQueryMotif(
            name="storage_to_postgresql_motif",
            dbapi_hook=PostgreSQLConnectorHook,
            storage_hook=origin_gcs_hook,
            destination_table=destination["uri"]
        )
