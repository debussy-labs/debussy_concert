from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.pipeline.transform.config.transform import ConfigTransformComposition
from debussy_concert.pipeline.transform.config.movement_parameters.dbt import DbtMovementParameters, DbtParameters
from debussy_concert.pipeline.transform.motif.run_transformation import DbtRunMotif
from debussy_concert.pipeline.transform.movement.transform import TransformationMovement
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase
from debussy_concert.pipeline.transform.phrase.data_lakehouse_transformation import DataLakehouseTransformationPhrase


class DbtTransformationComposition(CompositionBase):
    def dbt_transformation_builder(self, movement_parameters: DbtMovementParameters) -> TransformationMovement:
        data_warehouse_transformation_phrase = self.data_warehouse_transformation_phrase(
            dbt_run_parameters=movement_parameters.dbt_run_parameters
        )
        movement = TransformationMovement(
            start_phrase=StartPhrase(),
            data_warehouse_transformation_phrase=data_warehouse_transformation_phrase,
            end_phrase=EndPhrase()
        )
        movement.setup()

    def data_warehouse_transformation_phrase(self, dbt_run_parameters: DbtParameters):
        dbt_run_motif = DbtRunMotif(**dbt_run_parameters)
        transform_phrase = DataLakehouseTransformationPhrase(run_transformation_motif=dbt_run_motif)
        return transform_phrase
