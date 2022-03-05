from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.phrase.export_bigquery_table import ExportTablePhrase


class BigQueryToGcsMovement(MovementBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name,
                         config=config,
                         phrases=[ExportTablePhrase(config=config)])
