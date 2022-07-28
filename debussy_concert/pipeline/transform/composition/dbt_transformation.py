from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.pipeline.transform.config.transform import ConfigTransformComposition
from debussy_concert.pipeline.transform.config.movement_parameters.transform import DbtMovementParameters
from debussy_concert.pipeline.transform.movement.transform import TransformationMovement
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase


class DbtTransformationComposition(CompositionBase):
    def dbt_transformation(movement_parameters: DbtMovementParameters) -> TransformationMovement:
        data_warehouse_transformation_phrase = 1
        movement = TransformationMovement(
            start_phrase=StartPhrase(),
            data_warehouse_transformation_phrase=data_warehouse_transformation_phrase,
            end_phrase=EndPhrase()
        )
        movement.setup()
