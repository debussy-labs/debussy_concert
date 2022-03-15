from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow_concert.motif.motif_base import PMotif


class BigQueryJobMixin:
    def query_configuration(self, sql_query):
        return {
            "query": {
                "query": sql_query,
                "useLegacySql": False,
            }
        }

    def insert_job_operator(self: PMotif, dag, task_group, configuration):
        bigquery_job_operator = BigQueryInsertJobOperator(
            task_id=self.name,
            configuration=configuration,
            dag=dag,
            task_group=task_group
        )
        return bigquery_job_operator
