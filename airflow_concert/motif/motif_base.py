from abc import ABC, abstractmethod
from airflow.models.taskmixin import TaskMixin

from airflow_concert.config.config_integration import ConfigIntegration


class MotifBase(ABC):
    def __init__(self, config: ConfigIntegration, name=None) -> None:
        self.config = config
        self.name = name or self.__class__.__name__

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    @abstractmethod
    def build(self, dag, task_group) -> TaskMixin:
        pass
