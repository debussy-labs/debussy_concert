from airflow.providers.standard.operators.empty import EmptyOperator

from debussy.core.motif.motif_base import MotifBase


class EndMotif(MotifBase):
    def __init__(self, name=None) -> None:
        super().__init__(name=name)

    def build(self, dag, phrase_group):
        operator = EmptyOperator(task_id=self.name, dag=dag, task_group=phrase_group)
        return operator
