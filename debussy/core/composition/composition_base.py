from typing import Callable, List, Protocol, Sequence
import inject

from debussy.core.entities.protocols import PWorkflowDag
from debussy.core.movement.movement_base import PMovement
from debussy.core.config.config_composition import ConfigComposition
from debussy.core.config.movement_parameters.base import MovementParametersType
from debussy.core.service.workflow.protocol import PWorkflowService


class PComposition(Protocol):
    config: ConfigComposition
    workflow_service: PWorkflowService

    def play(self, *args, **kwargs) -> PWorkflowDag:
        pass

    def multi_play(self, *args, **kwargs) -> Sequence[PWorkflowDag]:
        pass

    def build_multi_dag(
        self, movement_builder: Callable[[MovementParametersType], PMovement]
    ) -> Sequence[PWorkflowDag]:
        pass

    def build(
        self, movement_builder: Callable[[MovementParametersType], PMovement]
    ) -> PWorkflowDag:
        pass


class CompositionBase(PComposition):
    @inject.params(config=ConfigComposition, workflow_service=PWorkflowService)
    def __init__(
        self, *, config: ConfigComposition, workflow_service: PWorkflowService
    ):
        self.config = config
        self.workflow_service = workflow_service

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def multi_play(self, *args, **kwargs):
        return self.build_multi_dag(*args, **kwargs)

    def build_multi_dag(
        self, movement_builder: Callable[[MovementParametersType], PMovement]
    ) -> List[PWorkflowDag]:
        dags = list()
        for movement_parameter in self.config.movements_parameters:
            name = self.config.dag_parameters.dag_id + "." + movement_parameter.name
            kwargs = {**self.config.dag_parameters}
            del kwargs["dag_id"]
            workflow_dag = self.workflow_service.workflow_dag(dag_id=name, **kwargs)
            movement_builder(movement_parameter).play(workflow_dag=workflow_dag)
            dags.append(workflow_dag)
        return dags

    def build(
        self, movement_builder: Callable[[MovementParametersType], PMovement]
    ) -> PWorkflowDag:
        name = self.config.dag_parameters.dag_id
        kwargs = {**self.config.dag_parameters}
        del kwargs["dag_id"]
        workflow_dag = self.workflow_service.workflow_dag(dag_id=name, **kwargs)

        for movement_parameter in self.config.movements_parameters:
            movement = movement_builder(movement_parameter)
            movement.build(workflow_dag=workflow_dag)
        return workflow_dag
