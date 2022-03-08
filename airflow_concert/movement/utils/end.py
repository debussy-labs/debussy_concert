from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.phrase.end import EndPhrase


class EndMovement(MovementBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(
            name=name,
            phrases=[EndPhrase(config=config)]
        )
