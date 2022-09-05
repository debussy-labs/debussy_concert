from typing import Callable, List, Protocol, Sequence
import inject

from airflow import DAG

from debussy_concert.core.movement.movement_base import PMovement
from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.core.config.movement_parameters.base import MovementParametersType
from debussy_concert.core.service.workflow.protocol import PWorkflowService


class PComposition(Protocol):
    config: ConfigComposition
    workflow_service: PWorkflowService

    def play(self, *args, **kwargs) -> DAG:
        pass

    def multi_play(self, *args, **kwargs) -> Sequence[DAG]:
        pass

    def build_multi_dag(
        self, movement_builder: Callable[[MovementParametersType], PMovement]
    ) -> Sequence[DAG]:
        pass

    def build(
        self, movement_builder: Callable[[MovementParametersType], PMovement]
    ) -> DAG:
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
    ) -> List[DAG]:
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
    ) -> DAG:
        name = self.config.dag_parameters.dag_id
        kwargs = {**self.config.dag_parameters}
        del kwargs["dag_id"]
        workflow_dag = self.workflow_service.workflow_dag(dag_id=name, **kwargs)

        for movement_parameter in self.config.movements_parameters:
            movement = movement_builder(movement_parameter)
            movement.build(workflow_dag=workflow_dag)
        return workflow_dag
