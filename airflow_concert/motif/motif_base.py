from abc import ABC, abstractmethod
from typing import Callable, Protocol
import inject

from airflow_concert.entities.protocols import PWorkflowDag, PMotifGroup
from airflow_concert.config.config_composition import ConfigComposition
from airflow_concert.service.workflow.protocol import PWorkflowService


class PMotif(Protocol):
    config: ConfigComposition
    name: str
    workflow_service: PWorkflowService

    def play(self, *args, **kwargs) -> PMotifGroup:
        pass

    def build(self, dag, phrase_group) -> PMotifGroup:
        pass


class PClusterMotifMixin(PMotif, Protocol):
    cluster_name: str
    cluster_config: dict


class MotifBase(PMotif, ABC):
    @inject.autoparams()
    def __init__(self, *,
                 workflow_service: PWorkflowService,
                 config: ConfigComposition, name=None) -> None:
        self.workflow_service = workflow_service
        self.config = config
        self.name = name or self.__class__.__name__

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def _build(self, workflow_dag, phrase_group, task_builder: Callable[[PWorkflowDag, PMotifGroup], None]):
        motif_group = self.workflow_service.motif_group(
            group_id=self.name, workflow_dag=workflow_dag, phrase_group=phrase_group)
        task_builder(workflow_dag, motif_group)
        return motif_group

    @abstractmethod
    def build(self, workflow_dag, phrase_group) -> PMotifGroup:
        pass


class DummyMotif(MotifBase):
    def __init__(self, config: ConfigComposition, name=None) -> None:
        super().__init__(config=config, name=name)

    def build(self, workflow_dag, phrase_group):
        from airflow.operators.dummy import DummyOperator
        operator = DummyOperator(task_id=self.name, dag=workflow_dag, task_group=phrase_group)
        return operator

    def setup(self, *args, **kwargs):
        pass
