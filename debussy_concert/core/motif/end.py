from airflow.operators.dummy import DummyOperator

from debussy_concert.core.motif.motif_base import MotifBase


class EndMotif(MotifBase):
    def __init__(self, name=None) -> None:
        super().__init__(name=name)

    def build(self, dag, phrase_group):
        operator = DummyOperator(task_id=self.name, dag=dag, task_group=phrase_group)
        return operator
