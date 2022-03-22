from abc import ABC, abstractmethod
from typing import Callable, Protocol
import inject

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.taskmixin import TaskMixin

from airflow_concert.config.config_composition import ConfigComposition
from airflow_concert.service.workflow.protocol import PWorkflowService


class PMotif(Protocol):
    config: ConfigComposition
    name: str
    workflow_service: PWorkflowService

    def play(self, *args, **kwargs) -> TaskMixin:
        pass

    def build(self, dag, phrase_group) -> TaskMixin:
        pass


class PClusterMotifMixin(PMotif, Protocol):
    cluster_name: str
    cluster_config: dict


class MotifBase(PMotif, ABC):
    @inject.autoparams()
    def __init__(self,
                 workflow_service: PWorkflowService,
                 config: ConfigComposition, name=None) -> None:
        self.workflow_service = workflow_service
        self.config = config
        self.name = name or self.__class__.__name__

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def _build(self, workflow_dag, phrase_group, task_builder: Callable[[DAG, TaskGroup], TaskMixin]):
        motif_group = self.workflow_service.motif_group(
            group_id=self.name, workflow_dag=workflow_dag, phrase_group=phrase_group)
        task_builder(workflow_dag, motif_group)
        return motif_group

    @abstractmethod
    def build(self, dag, phrase_group) -> TaskMixin:
        pass
