from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.reverse_etl.movement.reverse_etl import ReverseEtlMovement
from debussy_concert.config.reverse_etl import ConfigReverseEtl
from debussy_concert.config.movement_parameters.reverse_etl import ReverseEtlMovementParameters

from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase
from debussy_concert.reverse_etl.phrase.dw_to_reverse_etl import DataWarehouseToReverseEtlPhrase
from debussy_concert.reverse_etl.phrase.reverse_etl_to_storage import DataWarehouseReverseEtlToTempToStoragePhrase
from debussy_concert.reverse_etl.phrase.storage_to_destination import StorageToDestinationPhrase
from debussy_concert.reverse_etl.motif.bigquery_query_job import BigQueryQueryJobMotif
from debussy_concert.reverse_etl.motif.bigquery_extract_job import BigQueryExtractJobMotif
from debussy_concert.core.motif.mixins.bigquery_job import BigQueryTimePartitioning
from debussy_concert.reverse_etl.motif.storage_to_storage_motif import StorageToStorageMotif
from debussy_framework.v3.hooks.storage_hook import GCSHook


class ChildrensCorner(CompositionBase):
    def __init__(self, config: ConfigReverseEtl):
        super().__init__(config=config)

    def gcs_reverse_etl_movement_builder(self, movement_parameters: ReverseEtlMovementParameters):
        storage_to_destination_phrase = self.storage_to_destination_phrase(movement_parameters)
        return self.reverse_etl_movement_builder(
            storage_to_destination_phrase=storage_to_destination_phrase,
            movement_parameters=movement_parameters)

    def reverse_etl_movement_builder(self, storage_to_destination_phrase,
                                     movement_parameters: ReverseEtlMovementParameters) -> ReverseEtlMovement:
        start_phrase = StartPhrase(config=self.config)
        end_phrase = EndPhrase(config=self.config)
        data_warehouse_raw_to_reverse_etl_phrase = self.data_warehouse_raw_to_reverse_etl_phrase(
            partition_type=movement_parameters.reverse_etl_dataset_partition_type,
            partition_field=movement_parameters.reverse_etl_dataset_partition_field
        )
        data_warehouse_reverse_etl_to_storage_phrase = self.data_warehouse_reverse_etl_to_storage_phrase(
            destination_format=movement_parameters.file_format,
            field_delimiter=movement_parameters.field_delimiter
        )

        name = f'ReverseEtlMovement_{movement_parameters.name}'
        movement = ReverseEtlMovement(
            name=name,
            start_phrase=start_phrase,
            data_warehouse_to_reverse_etl_phrase=data_warehouse_raw_to_reverse_etl_phrase,
            data_warehouse_reverse_etl_to_storage_phrase=data_warehouse_reverse_etl_to_storage_phrase,
            storage_to_destination_phrase=storage_to_destination_phrase,
            end_phrase=end_phrase
        )
        movement.setup(self.config, movement_parameters)
        return movement

    def data_warehouse_raw_to_reverse_etl_phrase(self, partition_type, partition_field):
        time_partitioning = BigQueryTimePartitioning(type=partition_type, field=partition_field)
        bigquery_job = BigQueryQueryJobMotif(name='bq_to_reverse_etl_motif',
                                             write_disposition="WRITE_APPEND",
                                             create_disposition="CREATE_IF_NEEDED",
                                             time_partitioning=time_partitioning)
        phrase = DataWarehouseToReverseEtlPhrase(
            dw_to_reverse_etl_motif=bigquery_job
        )
        return phrase

    def data_warehouse_reverse_etl_to_storage_phrase(self, destination_format, field_delimiter):
        bigquery_job = BigQueryQueryJobMotif(name='bq_reverse_etl_to_temp_table_motif',
                                             write_disposition="WRITE_TRUNCATE",
                                             create_disposition="CREATE_IF_NEEDED")
        export_bigquery = BigQueryExtractJobMotif(name='bq_export_temp_table_to_gcs_motif',
                                                  destination_format=destination_format,
                                                  field_delimiter=field_delimiter)
        phrase = DataWarehouseReverseEtlToTempToStoragePhrase(
            name='DataWarehouseReverseEtlToStoragePhrase',
            datawarehouse_reverse_etl_to_temp_table_motif=bigquery_job,
            export_temp_table_to_storage_motif=export_bigquery
        )
        return phrase

    def storage_to_destination_phrase(self, movement_parameters: ReverseEtlMovementParameters):
        dest_conn_id = movement_parameters.destination_connection_id
        destiny_file_uri = movement_parameters.destination_object_path
        origin_gcs_hook = GCSHook(gcp_conn_id=dest_conn_id)
        destiny_gcs_hook = GCSHook(gcp_conn_id=dest_conn_id)
        storage_to_sftp = StorageToStorageMotif(
            name='gcs_to_gcs_motif',
            config=self.config,
            origin_storage_hook=origin_gcs_hook,
            destiny_storage_hook=destiny_gcs_hook,
            destiny_file_uri=destiny_file_uri
        )
        phrase = StorageToDestinationPhrase(
            storage_to_destination_motif=storage_to_sftp)
        return phrase

    def dummy_motif(self, name):
        from debussy_concert.core.motif.motif_base import DummyMotif
        return DummyMotif(config=None, name=name)

    @classmethod
    def create_from_yaml(cls, environment_config_yaml_filepath, composition_config_yaml_filepath):
        config = ConfigReverseEtl.load_from_file(
            composition_config_file_path=composition_config_yaml_filepath,
            env_file_path=environment_config_yaml_filepath
        )
        return cls(config)
