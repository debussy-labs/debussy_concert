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
        project_id, dataset_id, table_id = self.destination_table_uri.split(".")
        return [
            TaskSpec(
                name=self.name,
                action="bigquery.create_table",
                params={
                    "project_id": project_id,
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "table_resource": self.table_resource,
                    "gcp_conn_id": self.gcp_conn_id,
                    # Hive partitioning finds new partitions by itself, so an
                    # existing external table is kept as it is.
                    "if_exists": "ignore",
                },
                templated={"project_id", "dataset_id", "table_id", "table_resource"},
            )
        ]
