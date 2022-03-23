from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.movement.reverse_etl import ReverseEtlMovement
from airflow_concert.config.reverse_etl import ConfigReverseEtl
from airflow_concert.entities.table import Table

from airflow_concert.phrase.utils.start import StartPhrase
from airflow_concert.phrase.utils.end import EndPhrase
from airflow_concert.phrase.raw_to_reverse_etl import DataWarehouseToReverseEtlPhrase
from airflow_concert.phrase.reverse_etl_to_storage import DataWarehouseReverseEtlToTempToStoragePhrase
from airflow_concert.phrase.storage_to_destination import StorageToDestinationPhrase


class ReverseEtlComposition(CompositionBase):
    def __init__(self, config: ConfigReverseEtl):
        super().__init__(config=config)

    def reverse_etl_movement_builder(self, table: Table) -> ReverseEtlMovement:
        start_phrase = StartPhrase(config=self.config)
        end_phrase = EndPhrase(config=self.config)
        data_warehouse_raw_to_reverse_etl_phrase = self.data_warehouse_raw_to_reverse_etl_phrase()
        data_warehouse_reverse_etl_to_storage_phrase = self.data_warehouse_reverse_etl_to_storage_phrase()
        storage_to_destination_phrase = self.storage_to_destination_phrase()
        movement = ReverseEtlMovement(
            start_phrase=start_phrase,
            data_warehouse_to_reverse_etl_phrase=data_warehouse_raw_to_reverse_etl_phrase,
            data_warehouse_reverse_etl_to_storage_phrase=data_warehouse_reverse_etl_to_storage_phrase,
            storage_to_destination_phrase=storage_to_destination_phrase,
            end_phrase=end_phrase
        )
        movement.setup(self.config, table)
        return movement

    def data_warehouse_raw_to_reverse_etl_phrase(self):
        bigquery_job = self.dummy_motif('dw_to_reverse_etl_motif')
        phrase = DataWarehouseToReverseEtlPhrase(
            dw_to_reverse_etl_motif=bigquery_job
        )
        return phrase

    def data_warehouse_reverse_etl_to_storage_phrase(self):
        bigquery_job = self.dummy_motif('dw_reverse_etl_to_temp_table_motif')
        export_bigquery = self.dummy_motif('export_temp_table_to_storage_motif')
        phrase = DataWarehouseReverseEtlToTempToStoragePhrase(
            datawarehouse_reverse_etl_to_temp_table_motif=bigquery_job,
            export_temp_table_to_storage_motif=export_bigquery
        )
        return phrase

    def storage_to_destination_phrase(self):
        storage_to_sftp = self.dummy_motif('storage_to_sftp')
        phrase = StorageToDestinationPhrase(
            storage_to_destination_motif=storage_to_sftp)
        return phrase

    def dummy_motif(self, name):
        from airflow_concert.motif.motif_base import DummyMotif
        return DummyMotif(config=None, name=name)

    @classmethod
    def create_from_yaml(cls, environment_config_yaml_filepath, composition_config_yaml_filepath):
        config = ConfigReverseEtl.load_from_file(
            composition_config_file_path=composition_config_yaml_filepath,
            env_file_path=environment_config_yaml_filepath
        )
        return cls(config)
