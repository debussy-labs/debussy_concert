from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.phrase.start import StartPhrase


class StartMovement(MovementBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(
            name=name,
            config=config,
            phrases=[StartPhrase(config=config)]
        )
