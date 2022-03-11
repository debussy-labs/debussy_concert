from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.phrase.phrase_base import PhraseBase


class IngestionToLandingMovement(MovementBase):
    def __init__(self, export_table_phrase: PhraseBase, name=None) -> None:
        phrases = [export_table_phrase]
        super().__init__(name=name,
                         phrases=phrases)
