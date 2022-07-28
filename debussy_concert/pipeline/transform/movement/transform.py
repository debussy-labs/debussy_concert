from debussy_concert.core.movement.movement_base import MovementBase
from debussy_concert.core.movement.protocols import (
    PStartPhrase,
    PEndPhrase
)

from debussy_concert.core.config.movement_parameters.base import MovementParametersBase


class TransformationMovement(MovementBase):

    def __init__(
        self,
        start_phrase: PStartPhrase,
        # pre_condition_verification_phrase TODO
        # test_data_integrity_phrase TODO <-- pular etapa que gera o dado em produção em caso de falha nos testes. notificar? slack?
        data_warehouse_transformation_phrase,
        end_phrase: PEndPhrase,
        name=None
    ) -> None:

        self.start_phrase = start_phrase
        self.data_warehouse_transformation_phrase = data_warehouse_transformation_phrase
        self.end_phrase = end_phrase
        phrases = [
            self.start_phrase,
            self.data_warehouse_transformation_phrase,
            self.end_phrase
        ]
        super().__init__(name=name, phrases=phrases)

    def setup(
        self,
        movement_parameters: MovementParametersBase
    ):
        self.movement_parameters = movement_parameters
        self.data_warehouse_transformation_phrase.setup()
        return self
