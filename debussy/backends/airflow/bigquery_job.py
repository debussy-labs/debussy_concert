from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


class BigQueryInsertJobOperator(BigQueryInsertJobOperator):
    template_ext = (".json", ".sql")
