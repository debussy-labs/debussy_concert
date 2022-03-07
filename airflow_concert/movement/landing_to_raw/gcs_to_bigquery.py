from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.phrase.append_csv import AppendCsvPhrase
from airflow_concert.phrase.merge_csv import MergeCsvPhrase
from airflow.utils.task_group import TaskGroup


class GcsToBigQueryMovement(MovementBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name,
                         config=config,
                         phrases=[AppendCsvPhrase(config), MergeCsvPhrase(config)])

    def build(self, dag, parent_group) -> TaskGroup:
        task_group = TaskGroup(group_id=self.name, dag=dag, parent_group=parent_group)
        self.phrases[0].build(dag, task_group)
        self.phrases[1].build(dag, task_group)

        return task_group
