from airflow_concert.phrase.phrase_base import PhraseBase
from airflow.operators.dummy import DummyOperator


class AppendCsvPhrase(PhraseBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name, config=config)

    def build(self, dag, task_group):
        operator = DummyOperator(task_id=self.name, dag=dag, task_group=task_group)
        return operator
