from airflow.sdk import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateTableOperator,
    BigQueryUpdateTableOperator,
)
from debussy.backends.airflow.bigquery_job import TableReference
from debussy.core.motif.motif_base import MotifBase
from debussy.core.service.lakehouse.google_cloud import (
    GoogleCloudLakeHouseService,
    BigQueryTable,
)


class CreateBigQueryTableMotif(MotifBase):
    def __init__(self, gcp_conn_id, table: BigQueryTable, name=None):
        super().__init__(name=name)
        self.bq_schema = GoogleCloudLakeHouseService.get_table_schema(table)
        self.bq_partitioning = GoogleCloudLakeHouseService.get_table_partitioning(table)
        self.bq_table_resource = GoogleCloudLakeHouseService.get_table_resource(table)
        self.gcp_conn_id = gcp_conn_id

    def setup(self, table_uri):
        self.table_ref = TableReference(table_uri=table_uri)

    def build(self, workflow_dag, phrase_group):
        task_group = TaskGroup(
            group_id=self.name, dag=workflow_dag, parent_group=phrase_group
        )
        table_resource = {"schema": {"fields": self.bq_schema}}
        if self.bq_partitioning:
            table_resource["timePartitioning"] = self.bq_partitioning
        create_table = BigQueryCreateTableOperator(
            task_id="create_empty_table",
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.table_ref.project_id,
            dataset_id=self.table_ref.dataset_id,
            table_id=self.table_ref.table_id,
            table_resource=table_resource,
            if_exists="log",
            dag=workflow_dag,
            task_group=task_group,
        )
        update_table = BigQueryUpdateTableOperator(
            task_id="update_table_resources",
            dag=workflow_dag,
            task_group=task_group,
            table_resource=self.bq_table_resource,
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.table_ref.project_id,
            dataset_id=self.table_ref.dataset_id,
            table_id=self.table_ref.table_id,
        )
        self.workflow_service.chain_tasks(create_table, update_table)
        return task_group
