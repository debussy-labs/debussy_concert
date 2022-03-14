from typing import List
from airflow.utils.task_group import TaskGroup
from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.config.config_integration import ConfigIntegration


class MovementBase:
    def __init__(
        self, name,
        phrases: List[PhraseBase]
    ) -> None:
        self.name = name
        self.phrases = phrases

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def build(self, dag) -> TaskGroup:
        task_group = TaskGroup(group_id=self.name, dag=dag)
        current_task_group = self.phrases[0].build(dag, task_group)

        for phrase in self.phrases[1:]:
            phrase_task_group = phrase.build(dag, task_group)
            current_task_group >> phrase_task_group
            current_task_group = phrase_task_group
        return task_group
