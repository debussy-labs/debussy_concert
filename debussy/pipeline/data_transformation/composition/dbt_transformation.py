from debussy.core.composition.composition_base import CompositionBase
from debussy.core.phrase.utils.end import EndPhrase
from debussy.core.phrase.utils.start import StartPhrase
from debussy.pipeline.data_transformation.config.movement_parameters.dbt import (
    DbtMovementParameters,
    DbtParameters,
)
from debussy.pipeline.data_transformation.motif.run_transformation import (
    DbtRunMotif,
)
from debussy.pipeline.data_transformation.movement.transform import (
    TransformationMovement,
)
from debussy.pipeline.data_transformation.phrase.data_lakehouse_transformation import (
    DataLakehouseTransformationPhrase,
)


class DbtTransformationComposition(CompositionBase):
    def auto_play(self):
        return self.build(self.dbt_transformation_builder)

    def dbt_transformation_builder(
        self, movement_parameters: DbtMovementParameters
    ) -> TransformationMovement:
        data_warehouse_transformation_phrase = (
            self.data_warehouse_transformation_phrase(
                dbt_run_parameters=movement_parameters.dbt_run_parameters,
                movement_name=movement_parameters.name,
            )
        )
        movement = TransformationMovement(
            start_phrase=StartPhrase(),
            data_warehouse_transformation_phrase=data_warehouse_transformation_phrase,
            end_phrase=EndPhrase(),
        )
        movement.setup()
        return movement

    def data_warehouse_transformation_phrase(
        self, dbt_run_parameters: DbtParameters, movement_name
    ):
        dbt_run_motif = DbtRunMotif(
            dbt_run_parameters=dbt_run_parameters, movement_name=movement_name
        )

        transform_phrase = DataLakehouseTransformationPhrase(
            run_transformation_motif=dbt_run_motif
        )
        return transform_phrase
