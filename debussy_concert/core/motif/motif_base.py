from abc import ABC, abstractmethod
from typing import Callable, Protocol, TypeVar
import inject

from debussy_concert.core.entities.protocols import PWorkflowDag, PMotifGroup
from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.core.service.workflow.protocol import PWorkflowService

ConfigCompositionType = TypeVar('ConfigCompositionType', bound=ConfigComposition)


class PMotif(Protocol):
    name: str
    workflow_service: PWorkflowService

    @property
    def config(self) -> ConfigCompositionType:
        pass

    def play(self, *args, **kwargs) -> PMotifGroup:
        pass

    def build(self, dag, phrase_group) -> PMotifGroup:
        pass


class PClusterMotifMixin(PMotif, Protocol):
    cluster_name: str
    cluster_config: dict


class MotifBase(PMotif, ABC):
    @inject.params(config=ConfigComposition, workflow_service=PWorkflowService)
    def __init__(self, *,
                 workflow_service: PWorkflowService,
                 config: ConfigComposition,
                 name=None) -> None:
        self.workflow_service = workflow_service
        self._config = config
        self.name = name or self.__class__.__name__

    @property
    def config(self) -> ConfigCompositionType:
        return self._config

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
    def __init__(self, name=None) -> None:
        super().__init__(name=name)

    def build(self, workflow_dag, phrase_group):
        from airflow.operators.dummy import DummyOperator
        operator = DummyOperator(task_id=self.name, dag=workflow_dag, task_group=phrase_group)
        return operator

    def setup(self, *args, **kwargs):
        pass
