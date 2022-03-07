from typing import List
from airflow_concert.phrase.phrase_base import PhraseBase
from airflow.utils.task_group import TaskGroup


class MovementBase:
    def __init__(self, config, phrases: List[PhraseBase], name=None) -> None:
        self.config = config
        self.name = name or self.__class__.__name__
        self.phrases = phrases

    def build(self, dag, parent_task_group) -> TaskGroup:
        task_group = TaskGroup(group_id=self.name, dag=dag, parent_group=parent_task_group)
        current_task = self.phrases[0].build(dag, task_group)
        for phrase in self.phrases[1:]:
            phrase_task = phrase.build(dag, task_group)
            current_task >> phrase_task
            current_task = phrase_task
        return task_group
