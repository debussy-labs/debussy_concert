from typing import List
from airflow_concert.motif.motif_base import MotifBase
from airflow.utils.task_group import TaskGroup


class PhraseBase:
    def __init__(self, motifs: List[MotifBase] = None, name=None) -> None:
        self.name = name or self.__class__.__name__
        self.motifs = motifs or list()

    def add_motif(self, motif):
        self.motifs.append(motif)

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def build(self, dag, parent_task_group) -> TaskGroup:
        task_group = TaskGroup(group_id=self.name, dag=dag, parent_group=parent_task_group)
        current_task = self.motifs[0].build(dag, task_group)
        for motif in self.motifs[1:]:
            motif_task = motif.build(dag, task_group)
            current_task >> motif_task
            current_task = motif_task
        return task_group
