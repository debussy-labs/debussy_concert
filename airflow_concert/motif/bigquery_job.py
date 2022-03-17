from airflow_concert.motif.motif_base import MotifBase
from airflow_concert.motif.mixins.bigquery_job import BigQueryJobMixin
from airflow_concert.phrase.protocols import PExecuteQueryMotif


class BigQueryJobMotif(MotifBase, BigQueryJobMixin, PExecuteQueryMotif):
    def __init__(self, sql_query=None, config=None, name=None) -> None:
        super().__init__(name=name, config=config)
        self.sql_query = sql_query

    def setup(self, sql_query: str):
        self.sql_query = sql_query

    def build(self, dag, task_group):
        bigquery_job_operator = self.insert_job_operator(
            dag, task_group,
            self.query_configuration(sql_query=self.sql_query)
        )
        return bigquery_job_operator
