from airflow_concert.movement.movement_base import MovementBase


class DataIngestionMovement(MovementBase):
    def __init__(
        self, name,
        start_phrase,
        ingestion_source_to_landing_storage_phrase,
        landing_storage_to_data_warehouse_raw_phrase,
        data_warehouse_raw_to_trusted_phrase,
        end_phrase
    ) -> None:

        phrases = [
            start_phrase,
            ingestion_source_to_landing_storage_phrase,
            landing_storage_to_data_warehouse_raw_phrase,
            data_warehouse_raw_to_trusted_phrase,
            end_phrase
        ]
        super().__init__(name, phrases)
