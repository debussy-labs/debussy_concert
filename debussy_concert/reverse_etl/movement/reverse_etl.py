from debussy_concert.core.movement.movement_base import MovementBase
from debussy_concert.core.movement.protocols import (
    PStartPhrase,
    PEndPhrase
)
from debussy_concert.reverse_etl.config.reverse_etl import ConfigReverseEtl
from debussy_concert.reverse_etl.config.movement_parameters.reverse_etl import ReverseEtlMovementParameters


class ReverseEtlMovement(MovementBase):
    config: ConfigReverseEtl

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
                f"{self.config.name}_{self.movement_parameters.name}")

    @property
    def reverse_etl_bucket_uri_prefix(self):
        return (f"gs://{self.config.environment.reverse_etl_bucket}/"
                f"{self.config.name}/{self.movement_parameters.name}/"
                f"{self.movement_parameters.output_config.file_name}")

    @property
    def datawarehouse_to_reverse_etl_query(self):
        return self.movement_parameters.reverse_etl_query

    @property
    def datawarehouse_reverse_etl_extraction_query(self):
        return self.movement_parameters.extraction_query_from_temp.format(reverse_etl_table_uri=self.reverse_etl_table_uri)

    def setup(
        self,
        movement_parameters: ReverseEtlMovementParameters
    ):
        self.movement_parameters = movement_parameters
        self.data_warehouse_to_reverse_etl_phrase.setup(
            reverse_etl_query=self.datawarehouse_to_reverse_etl_query,
            reverse_etl_table_uri=self.reverse_etl_table_uri)
        self.data_warehouse_reverse_etl_to_storage_phrase.setup(
            movement_parameters=self.movement_parameters,
            extraction_query=self.datawarehouse_reverse_etl_extraction_query,
            storage_uri_prefix=self.reverse_etl_bucket_uri_prefix)
        self.storage_to_destination_phrase.setup(
            storage_uri_prefix=self.reverse_etl_bucket_uri_prefix)
        return self
