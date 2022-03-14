from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.movement.protocols import (
    PStartPhrase,
    PIngestionSourceToLandingStoragePhrase,
    PLandingStorageToDataWarehouseRawPhrase,
    PDataWarehouseRawToTrustedPhrase,
    PEndPhrase
)


class DataIngestionMovement(MovementBase):
    def __init__(
        self, name,
        start_phrase: PStartPhrase,
        ingestion_source_to_landing_storage_phrase: PIngestionSourceToLandingStoragePhrase,
        landing_storage_to_data_warehouse_raw_phrase: PLandingStorageToDataWarehouseRawPhrase,
        data_warehouse_raw_to_trusted_phrase: PDataWarehouseRawToTrustedPhrase,
        end_phrase: PEndPhrase
    ) -> None:

        phrases = [
            start_phrase,
            ingestion_source_to_landing_storage_phrase,
            landing_storage_to_data_warehouse_raw_phrase,
            data_warehouse_raw_to_trusted_phrase,
            end_phrase
        ]
        super().__init__(name=name, phrases=phrases)
