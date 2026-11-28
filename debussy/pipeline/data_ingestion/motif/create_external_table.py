from debussy.core.motif.motif_base import MotifBase
from debussy.core.phrase.protocols import PCreateExternalTableMotif
from debussy.core.tasks import TaskSpec


class CreateExternalBigQueryTableMotif(MotifBase, PCreateExternalTableMotif):
    group_tasks = False

    def __init__(self, gcp_conn_id="google_cloud_default", name=None) -> None:
        super().__init__(name=name)
        self.gcp_conn_id = gcp_conn_id

    @property
    def table_resource(self):
        return {
            "type": "EXTERNAL",
            "externalDataConfiguration": {
                "hivePartitioningOptions": {
                    "mode": "AUTO",
                    "sourceUriPrefix": self.source_storage_uri_prefix,
                },
                "sourceFormat": "PARQUET",
                "sourceUris": [f"{self.source_storage_uri_prefix}/*.parquet"],
            },
        }

    def setup(
        self, source_bucket_uri_prefix: str, destination_project_dataset_table: str
    ):
        self.source_storage_uri_prefix = source_bucket_uri_prefix
        self.destination_table_uri = destination_project_dataset_table
        return self

    def tasks(self):
        return [
            TaskSpec(
                name=self.name,
                action="bigquery.create_external_table",
                params={
                    "bucket": self.source_storage_uri_prefix,
                    "destination_project_dataset_table": self.destination_table_uri,
                    "table_resource": self.table_resource,
                    "bigquery_conn_id": self.gcp_conn_id,
                },
                templated={
                    "bucket",
                    "destination_project_dataset_table",
                    "table_resource",
                },
            )
        ]
