from abc import ABC, abstractmethod
from typing import Callable

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.taskmixin import TaskMixin

from airflow_concert.config.config_integration import ConfigIntegration


class MotifBase(ABC):
    def __init__(self, config: ConfigIntegration, name=None) -> None:
        self.config = config
        self.name = name or self.__class__.__name__

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def _build(self, dag, task_group, task_builder: Callable[[DAG, TaskGroup], TaskMixin]):
        task_group = TaskGroup(group_id=self.name, dag=dag, parent_group=task_group)
        task_builder(dag, task_group)
        return task_group

    @abstractmethod
    def build(self, dag, task_group) -> TaskMixin:
        pass
