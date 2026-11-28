from debussy.core.motif.motif_base import MotifBase
from debussy.core.service.lakehouse.bigquery_job import TableReference
from debussy.core.service.lakehouse.google_cloud import (
    BigQueryTable,
    GoogleCloudLakeHouseService,
)
from debussy.core.tasks import TaskSpec


class CreateBigQueryTableMotif(MotifBase):
    def __init__(self, gcp_conn_id, table: BigQueryTable, name=None):
        super().__init__(name=name)
        self.bq_schema = GoogleCloudLakeHouseService.get_table_schema(table)
        self.bq_partitioning = GoogleCloudLakeHouseService.get_table_partitioning(table)
        self.bq_table_resource = GoogleCloudLakeHouseService.get_table_resource(table)
        self.gcp_conn_id = gcp_conn_id

    def setup(self, table_uri):
        self.table_ref = TableReference(table_uri=table_uri)

    def tasks(self):
        table = {
            "gcp_conn_id": self.gcp_conn_id,
            "project_id": self.table_ref.project_id,
            "dataset_id": self.table_ref.dataset_id,
            "table_id": self.table_ref.table_id,
        }
        table_resource = {"schema": {"fields": self.bq_schema}}
        if self.bq_partitioning:
            table_resource["timePartitioning"] = self.bq_partitioning
        return [
            TaskSpec(
                name="create_empty_table",
                action="bigquery.create_table",
                params={**table, "table_resource": table_resource, "if_exists": "log"},
            ),
            TaskSpec(
                name="update_table_resources",
                action="bigquery.update_table",
                params={**table, "table_resource": self.bq_table_resource},
            ),
        ]
