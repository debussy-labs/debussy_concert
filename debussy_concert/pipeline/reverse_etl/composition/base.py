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
from debussy_concert.pipeline.reverse_etl.motif.bigquery_query_job import (
    BigQueryQueryJobMotif,
)
from debussy_concert.pipeline.reverse_etl.motif.bigquery_extract_job import (
    BigQueryExtractJobMotif,
)


class ReverseEtlBigQueryCompositionBase(CompositionBase):
    config: ConfigReverseEtl

    def reverse_etl_movement_builder(
        self,
        movement_parameters: ReverseEtlMovementParameters,
        storage_to_destination_phrases,
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
        name = f"ReverseEtlMovement_{movement_parameters.name}"
        movement = ReverseEtlMovement(
            name=name,
            start_phrase=start_phrase,
            data_warehouse_to_reverse_etl_phrase=data_warehouse_raw_to_reverse_etl_phrase,
            data_warehouse_reverse_etl_to_storage_phrase=data_warehouse_reverse_etl_to_storage_phrase,
            storage_to_destination_phrases=storage_to_destination_phrases,
            end_phrase=end_phrase,
        )
        movement.setup(movement_parameters)
        return movement

    def data_warehouse_raw_to_reverse_etl_phrase(
        self, partition_type, partition_field, gcp_conn_id
    ):
        """
        Constructs the reverse ETL phrase that extracts data from data warehouse's raw to reverse_etl dataset.
        """
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
        """
        Constructs the reverse ETL phrase that extracts data from data warehouse's reverse_etl dataset to
        the reverse_etl storage (gcs bucket).
        """
        # mapping the format config to the respective motif function
        export_format = destination_config.format.lower()
        export_format_fn_map = {
            "csv": self.extract_csv_motif
        }
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
            print_header=False if destination_config.print_header is None else destination_config.print_header,
            gcp_conn_id=gcp_conn_id,
        )
        return export_bigquery
