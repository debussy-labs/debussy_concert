from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase
from debussy_concert.core.motif.mixins.bigquery_job import BigQueryTimePartitioning
from debussy_concert.pipeline.data_ingestion.composition.base import DataIngestionBase
from debussy_concert.pipeline.data_ingestion.movement.data_ingestion import DataIngestionMovement
from debussy_concert.pipeline.data_ingestion.config.bigquery_data_ingestion import ConfigBigQueryDataIngestion
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.time_partitioned import TimePartitionedDataIngestionMovementParameters
from debussy_concert.pipeline.reverse_etl.phrase.dw_to_reverse_etl import (
    DataWarehouseToReverseEtlPhrase,
)
from debussy_concert.pipeline.reverse_etl.phrase.reverse_etl_to_storage import (
    DataWarehouseReverseEtlToTempToStoragePhrase,
)
from debussy_concert.pipeline.reverse_etl.phrase.storage_to_destination import (
    StorageToDestinationPhrase,
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

from debussy_airflow.debussy_airflow.hooks.storage_hook import GCSHook, SFTPHook

class GcsIngestionComposition(DataIngestionBase):
    config: ConfigBigQueryDataIngestion

    def storage_ingestion_movement_builder(
       self, movement_parameters: TimePartitionedDataIngestionMovementParameters
    ) -> DataIngestionMovement:

        mysql_jdbc_driver = "com.mysql.cj.jdbc.Driver"
        mysql_jdbc_url = "jdbc:mysql://{host}:{port}/" + self.config.source_name

        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.rdbms_ingestion_to_raw_vault_phrase(
                mysql_jdbc_driver, mysql_jdbc_url, movement_parameters
            ),
        )

    def storage_to_destination_phrase(
        self, movement_parameters: TimePartitionedDataIngestionMovementParameters
    ):
        destination_file_uri = movement_parameters.destination_uri
        destination_type = movement_parameters.destination_type.lower()
        destination_type_motif_map = {
            "gcs": GCSHook,
            "sftp": SFTPHook,
        }
        HookCls = destination_type_motif_map[destination_type]
        origin_gcs_hook = GCSHook(
            gcp_conn_id=self.config.environment.data_lakehouse_connection_id
        )
        destination_hook = HookCls(movement_parameters.destination_connection_id)
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

    def reverse_etl_movement_builder(
        self,
        storage_to_destination_phrase,
        movement_parameters: ReverseEtlMovementParameters,
    ) -> DataIngestionMovement:
        start_phrase = StartPhrase()
        end_phrase = EndPhrase()
        name = f"GcsIngestionMovement_{movement_parameters.name}"
        movement = DataIngestionMovement(
            name=name,
            start_phrase=start_phrase,
            storage_to_destination_phrase=storage_to_destination_phrase,
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
        phrase = DataWarehouseToReverseEtlPhrase(dw_to_reverse_etl_motif=bigquery_job)
        return phrase

    def data_warehouse_reverse_etl_to_storage_phrase(
        self, destination_config: OutputConfig, gcp_conn_id
    ):
        # mapping the format config to the respective motif function
        export_format = destination_config.format.lower()
        export_format_fn_map = {"csv": self.extract_csv_motif}
        export_format_fn = export_format_fn_map[export_format]

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

    def extract_csv_motif(self, destination_config: CsvFile, gcp_conn_id):
        export_bigquery = BigQueryExtractJobMotif(
            name="bq_export_temp_table_to_gcs_motif",
            destination_format=destination_config.format,
            field_delimiter=destination_config.field_delimiter,
            print_header=destination_config.print_header,
            gcp_conn_id=gcp_conn_id,
        )

        return export_bigquery

    
