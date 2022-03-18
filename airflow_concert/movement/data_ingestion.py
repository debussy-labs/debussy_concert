from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.movement.protocols import (
    PStartPhrase,
    PIngestionSourceToLandingStoragePhrase,
    PLandingStorageToDataWarehouseRawPhrase,
    PDataWarehouseRawToTrustedPhrase,
    PEndPhrase
)
from airflow_concert.config.config_composition import ConfigComposition
from airflow_concert.entities.table import Table


class DataIngestionMovement(MovementBase):
    def __init__(
        self, name,
        start_phrase: PStartPhrase,
        ingestion_source_to_landing_storage_phrase: PIngestionSourceToLandingStoragePhrase,
        landing_storage_to_data_warehouse_raw_phrase: PLandingStorageToDataWarehouseRawPhrase,
        data_warehouse_raw_to_trusted_phrase: PDataWarehouseRawToTrustedPhrase,
        end_phrase: PEndPhrase
    ) -> None:

        self.start_phrase = start_phrase
        self.ingestion_source_to_landing_storage_phrase = ingestion_source_to_landing_storage_phrase
        self.landing_storage_to_data_warehouse_raw_phrase = landing_storage_to_data_warehouse_raw_phrase
        self.data_warehouse_raw_to_trusted_phrase = data_warehouse_raw_to_trusted_phrase
        self.end_phrase = end_phrase
        phrases = [
            self.start_phrase,
            self.ingestion_source_to_landing_storage_phrase,
            self.landing_storage_to_data_warehouse_raw_phrase,
            self.data_warehouse_raw_to_trusted_phrase,
            self.end_phrase
        ]
        super().__init__(name=name, phrases=phrases)

    @property
    def landing_bucket_uri_prefix(self):
        return (f"gs://{self.config.environment.landing_bucket}/"
                f"{self.config.rdbms_name}/{self.config.database}/{self.table.name}")

    @property
    def raw_table_uri(self):
        return (f"{self.config.environment.project}."
                f"{self.config.environment.raw_dataset}."
                f"{self.config.table_prefix}_{self.table.name}")

    def setup(
        self,
        config: ConfigComposition,
        table: Table
    ):
        self.config = config
        self.table = table
        self.ingestion_source_to_landing_storage_phrase.setup(
            destination_storage_uri=self.landing_bucket_uri_prefix)
        self.landing_storage_to_data_warehouse_raw_phrase.setup(
            config=config, table=table,
            source_storage_uri_prefix=self.landing_bucket_uri_prefix,
            datawarehouse_raw_uri=self.raw_table_uri)
