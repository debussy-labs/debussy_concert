from airflow_concert.movement.movement_base import MovementBase


class GcsLandingToBigQueryRawMovement(MovementBase):

    def __init__(self, merge_landing_to_raw, name=None) -> None:
        phrases = [merge_landing_to_raw]
        super().__init__(name=name,
                         phrases=phrases)
