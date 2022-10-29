from typing import Optional
from debussy_concert.core.service.lakehouse.google_cloud import GoogleCloudLakeHouseService
from debussy_concert.core.entities.table import BigQueryTable
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.core.motif.mixins.bigquery_job import (
    BigQueryJobMixin,
    HivePartitioningOptions,
)


class LoadGcsToBigQueryHivePartitionMotif(MotifBase, BigQueryJobMixin):
    def __init__(
        self,
        *,
        source_format: str,
        gcs_partition: str,
        destination_partition: str,
        write_disposition: Optional[str] = None,
        create_disposition: Optional[str] = None,
        hive_partitioning_options: Optional[HivePartitioningOptions] = None,
        table_definition: Optional[BigQueryTable] = None,
        schema_update_options=None,
        gcp_conn_id="google_cloud_default",
        name=None,
        **op_kw_args,
    ) -> None:
        super().__init__(name=name)
        self.source_format = source_format
        self.gcs_partition = gcs_partition
        self.destination_partition = destination_partition
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.table_definition = table_definition
        self.hive_partitioning_options = hive_partitioning_options
        self.schema_update_options = schema_update_options
        self.gcp_conn_id = gcp_conn_id
        self.op_kw_args = op_kw_args

    def setup(self, source_storage_uri_prefix: str, destination_table_uri=None):
        self.source_uri = (
            f"{source_storage_uri_prefix}/" f"{self.gcs_partition}/" f"*.parquet"
        )
        self.destination_table = f"{destination_table_uri}${self.destination_partition}"

    def build(self, dag, phrase_group):
        schema = GoogleCloudLakeHouseService.get_table_schema(
            self.table_definition)
        schema = {'fields': [
            field for field in schema if not field['name'].startswith('_')]}

        bigquery_job_operator = self.insert_job_operator(
            dag,
            phrase_group,
            self.load_configuration(
                destination_table=self.destination_table,
                source_uris=self.source_uri,
                source_format=self.source_format,
                write_disposition=self.write_disposition,
                create_disposition=self.create_disposition,
                schema=schema,
                schema_update_options=self.schema_update_options,
                hive_partitioning_options=self.hive_partitioning_options,
            ),
            self.gcp_conn_id,
            **self.op_kw_args,
        )
        return bigquery_job_operator
