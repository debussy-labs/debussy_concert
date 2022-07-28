from airflow_dbt.operators.dbt_operator import DbtRunOperator

from debussy_concert.core.motif.motif_base import MotifBase


class CreateBigQueryTableMotif(MotifBase):
    def __init__(self, name=None, **dbt_args):
        self.dbt_args = dbt_args
        super().__init__(name=name)

    def setup(self):
        ...

    def build(self, workflow_dag, phrase_group):
        run_dbt = DbtRunOperator(
            **self.dbt_args,
            dag=workflow_dag,
            task_group=phrase_group,
        )
        return run_dbt
