from airflow_concert.motif.motif_base import MotifBase
from airflow_concert.phrase.protocols import PRawToTrustedMotif
from airflow.operators.dummy import DummyOperator


class BigQueryToBigQueryMotif(MotifBase, PRawToTrustedMotif):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name, config=config)

    def build(self, dag, task_group):
        operator = DummyOperator(task_id=self.name, dag=dag, task_group=task_group)
        return operator
