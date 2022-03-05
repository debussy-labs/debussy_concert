from abc import ABC, abstractmethod
from airflow.models.taskmixin import TaskMixin


class PhraseBase(ABC):
    def __init__(self, config, name=None) -> None:
        self.config = config
        self.name = name or self.__class__.__name__

    @abstractmethod
    def build(self, dag, task_group) -> TaskMixin:
        pass
