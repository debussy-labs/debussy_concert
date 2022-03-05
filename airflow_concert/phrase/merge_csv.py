from airflow_concert.phrase.phrase_base import PhraseBase
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup


class MergeCsvPhrase(PhraseBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name, config=config)

    def build(self, dag, task_group):
        sub_task_group = TaskGroup(group_id=self.name, dag=dag, parent_group=task_group)
        op1 = DummyOperator(task_id='CreateExternalTable', dag=dag, task_group=sub_task_group)
        op2 = DummyOperator(task_id='BuildMergeQuery', dag=dag, task_group=sub_task_group)
        op3 = DummyOperator(task_id='RunMergeQuery', dag=dag, task_group=sub_task_group)
        op1 >> op2 >> op3
        return sub_task_group
