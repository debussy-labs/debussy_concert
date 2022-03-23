from debussy_concert.movement.movement_base import MovementBase
from debussy_concert.movement.protocols import (
    PStartPhrase,
    PEndPhrase
)
from debussy_concert.config.reverse_etl import ConfigReverseEtl
from debussy_concert.entities.table import Table


class ReverseEtlMovement(MovementBase):
    def __init__(
        self,
        start_phrase: PStartPhrase,
        data_warehouse_to_reverse_etl_phrase,
        data_warehouse_reverse_etl_to_storage_phrase,
        storage_to_destination_phrase,
        end_phrase: PEndPhrase,
        name=None
    ) -> None:

        self.start_phrase = start_phrase
        self.data_warehouse_to_reverse_etl_phrase = data_warehouse_to_reverse_etl_phrase
        self.data_warehouse_reverse_etl_to_storage_phrase = data_warehouse_reverse_etl_to_storage_phrase
        self.storage_to_destination_phrase = storage_to_destination_phrase
        self.end_phrase = end_phrase
        phrases = [
            self.start_phrase,
            self.data_warehouse_to_reverse_etl_phrase,
            self.data_warehouse_reverse_etl_to_storage_phrase,
            self.storage_to_destination_phrase,
            self.end_phrase
        ]
        super().__init__(name=name, phrases=phrases)

    @property
    def reverse_etl_table_uri(self):
        return (f"{self.config.environment.project}."
                f"{self.config.environment.reverse_etl_dataset}."
                f"{self.config.name}_{self.table.name}")

    @property
    def reverse_etl_bucket_uri_prefix(self):
        return (f"gs://{self.config.environment.reverse_etl_bucket}/"
                f"{self.config.name}/{self.table.name}/{self.table.name}_"
                "{{ execution_date }}.csv")

    @property
    def datawarehouse_to_reverse_etl_query(self):
        return "select 'reverse_etl_composition_test' as data, '{{ ts_nodash }}' as date"

    @property
    def datawarehouse_reverse_etl_extract_query(self):
        return f"select * from `{self.reverse_etl_table_uri}`"

    def setup(
        self,
        config: ConfigReverseEtl,
        table: Table
    ):
        self.config = config
        self.table = table
        self.data_warehouse_to_reverse_etl_phrase.setup(
            reverse_etl_query=self.datawarehouse_to_reverse_etl_query,
            reverse_etl_table_uri=self.reverse_etl_table_uri)
        self.data_warehouse_reverse_etl_to_storage_phrase.setup(
            config=self.config, table=self.table,
            extract_query=self.datawarehouse_reverse_etl_extract_query,
            storage_uri_prefix=self.reverse_etl_bucket_uri_prefix)
        self.storage_to_destination_phrase.setup(
            storage_uri_prefix=self.reverse_etl_bucket_uri_prefix
        )
