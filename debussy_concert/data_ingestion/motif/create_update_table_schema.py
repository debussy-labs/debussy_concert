from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from debussy_concert.core.motif.mixins.bigquery_job import TableReference
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.core.service.lakehouse.google_cloud import GoogleCloudLakeHouseService, BigQueryTable


class CreateBigQueryTableMotif(MotifBase):
    def __init__(self, table: BigQueryTable, name=None):
        super().__init__(name=name)
        self.bq_schema = GoogleCloudLakeHouseService.get_table_schema(table)
        self.bq_partitioning = GoogleCloudLakeHouseService.get_table_partitioning(table)

    def setup(self, destination_table_uri):
        self.destination_table_uri = destination_table_uri
        self.table_ref = TableReference(table_uri=destination_table_uri)

    def build(self, workflow_dag, phrase_group):
        create_table = BigQueryCreateEmptyTableOperator(
            task_id='create_empty_table',
            project_id=self.table_ref.project_id,
            dataset_id=self.table_ref.dataset_id,
            table_id=self.table_ref.table_id,
            schema_fields=self.bq_schema,
            time_partitioning=self.bq_partitioning,
            dag=workflow_dag,
            task_group=phrase_group
        )
        return create_table
