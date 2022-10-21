from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase
from debussy_concert.core.motif.mixins.bigquery_job import BigQueryTimePartitioning
from debussy_concert.pipeline.reverse_etl.movement.reverse_etl import ReverseEtlMovement
from debussy_concert.pipeline.reverse_etl.config.reverse_etl import ConfigReverseEtl
from debussy_concert.pipeline.reverse_etl.config.movement_parameters.reverse_etl import (
    OutputConfig,
    CsvFile,
    ReverseEtlMovementParameters,
)
from debussy_concert.pipeline.reverse_etl.phrase.dw_to_reverse_etl import (
    DataWarehouseToReverseEtlPhrase,
)
from debussy_concert.pipeline.reverse_etl.phrase.reverse_etl_to_storage import (
    DataWarehouseReverseEtlToTempToStoragePhrase,
)
from debussy_concert.pipeline.reverse_etl.phrase.storage_to_destination import (
    StorageToDestinationPhrase,
)
from debussy_concert.pipeline.reverse_etl.motif.storage_to_rdbms_motif import (
    StorageToRdbmsQueryMotif,
)
from debussy_concert.pipeline.reverse_etl.motif.bigquery_query_job import (
    BigQueryQueryJobMotif,
)
from debussy_concert.pipeline.reverse_etl.motif.bigquery_extract_job import (
    BigQueryExtractJobMotif,
)
from debussy_concert.pipeline.reverse_etl.motif.storage_to_storage_motif import (
    StorageToStorageMotif,
)

from debussy_airflow.hooks.storage_hook import GCSHook, SFTPHook
from debussy_airflow.hooks.db_api_hook import MySqlConnectorHook


class ReverseEtlBigQueryToDestinationsComposition(CompositionBase):
    config: ConfigReverseEtl

    def bigquery_to_destinations_reverse_etl_movement_builder(
        self, movement_parameters: ReverseEtlMovementParameters
    ):
        reverse_etl_storage_to_destination_phrases = self.reverse_etl_storage_to_destination_phrases(
            movement_parameters
        )
        return self.reverse_etl_movement_builder(
            storage_to_destination_phrases=reverse_etl_storage_to_destination_phrases,
            movement_parameters=movement_parameters,
        )

    def reverse_etl_movement_builder(
        self,
        storage_to_destination_phrases,
        movement_parameters: ReverseEtlMovementParameters,
    ) -> ReverseEtlMovement:
        start_phrase = StartPhrase()
        end_phrase = EndPhrase()
        output_config: OutputConfig = movement_parameters.output_config
        data_warehouse_raw_to_reverse_etl_phrase = (
            self.data_warehouse_raw_to_reverse_etl_phrase(
                partition_type=movement_parameters.reverse_etl_dataset_partition_type,
                partition_field=movement_parameters.reverse_etl_dataset_partition_field,
                gcp_conn_id=self.config.environment.data_lakehouse_connection_id,
            )
        )

        data_warehouse_reverse_etl_to_storage_phrase = (
            self.data_warehouse_reverse_etl_to_storage_phrase(
                destination_config=output_config,
                gcp_conn_id=self.config.environment.data_lakehouse_connection_id,
            )
        )

        reverse_etl_storage_to_destination_phrases = (
            self.reverse_etl_storage_to_destination_phrases(
                movement_parameters
            )
        )

        name = f"ReverseEtlMovement_{movement_parameters.name}"
        movement = ReverseEtlMovement(
            name=name,
            start_phrase=start_phrase,
            data_warehouse_to_reverse_etl_phrase=data_warehouse_raw_to_reverse_etl_phrase,
            data_warehouse_reverse_etl_to_storage_phrase=data_warehouse_reverse_etl_to_storage_phrase,
            storage_to_destination_phrases=reverse_etl_storage_to_destination_phrases,
            end_phrase=end_phrase,
        )
        movement.setup(movement_parameters)
        return movement

    def data_warehouse_raw_to_reverse_etl_phrase(
        self, partition_type, partition_field, gcp_conn_id
    ):
        time_partitioning = BigQueryTimePartitioning(
            type=partition_type, field=partition_field
        )
        bigquery_job = BigQueryQueryJobMotif(
            name="bq_to_reverse_etl_motif",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning=time_partitioning,
            gcp_conn_id=gcp_conn_id,
        )
        phrase = DataWarehouseToReverseEtlPhrase(
            dw_to_reverse_etl_motif=bigquery_job)
        return phrase

    def data_warehouse_reverse_etl_to_storage_phrase(
        self, destination_config: OutputConfig, gcp_conn_id
    ):
        # mapping the format config to the respective motif function
        export_format = destination_config.format.lower()
        map_ = {
            "csv": self.extract_csv_motif
        }
        export_format_fn = map_.get(export_format)

        bigquery_job = BigQueryQueryJobMotif(
            name="bq_reverse_etl_to_temp_table_motif",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            gcp_conn_id=gcp_conn_id,
        )

        export_bigquery = export_format_fn(destination_config, gcp_conn_id)
        phrase = DataWarehouseReverseEtlToTempToStoragePhrase(
            name="DataWarehouseReverseEtlToStoragePhrase",
            datawarehouse_reverse_etl_to_temp_table_motif=bigquery_job,
            export_temp_table_to_storage_motif=export_bigquery,
        )
        return phrase

    def reverse_etl_storage_to_destination_phrases(
        self, movement_parameters: ReverseEtlMovementParameters
    ):
        destination_phrases = []
        for destination in movement_parameters.destinations:
            destination_name = destination["name"]
            destination_file_uri = destination["uri"]
            destination_type = destination["type"].lower()
            destination_type_motif_map = {
                "gcs": {"origin_hook": GCSHook, "destination_type": "storage"},
                "sftp": {"origin_hook": SFTPHook, "destination_type": "storage"},
                "mysql": {"origin_hook": MySqlConnectorHook, "destination_type": "rdbms"},
            }
            HookCls = destination_type_motif_map[destination_type]["origin_hook"]

            origin_gcs_hook = GCSHook(
                gcp_conn_id=self.config.environment.data_lakehouse_connection_id
            )

            destination_hook = HookCls(destination["connection_id"])

            destination_category = destination_type_motif_map[destination_type]["destination_type"]
            if destination_category == 'storage':
                storage_to_destination_motif = StorageToStorageMotif(
                    name=f"gcs_to_{destination_type}_motif",
                    origin_storage_hook=origin_gcs_hook,
                    destiny_storage_hook=destination_hook,
                    destiny_file_uri=destination_file_uri,
                )
            elif destination_category == 'rdbms':
                storage_to_destination_motif = StorageToRdbmsQueryMotif(
                    name=f"gcs_to_{destination_type}_motif",
                    dbapi_hook=destination_hook,
                    storage_hook=origin_gcs_hook,
                    destination_table=movement_parameters.destination_uri,
                )

            phrase = StorageToDestinationPhrase(
                name=f"StorageToDestinationPhrase_{destination_name}",
                storage_to_destination_motif=storage_to_destination_motif
            )
            destination_phrases.append(phrase)
        return destination_phrases

    def extract_csv_motif(self, destination_config: CsvFile, gcp_conn_id):
        export_bigquery = BigQueryExtractJobMotif(
            name="bq_export_temp_table_to_gcs_motif",
            destination_format=destination_config.format,
            field_delimiter=destination_config.field_delimiter,
            print_header=destination_config.print_header,
            gcp_conn_id=gcp_conn_id,
        )

        return export_bigquery
