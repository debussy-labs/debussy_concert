import re
from typing import Optional
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow_concert.entities.table import Table
from airflow_concert.motif.motif_base import PMotif


class TableReference:
    """
     NOTE: might exist an implementation for this in the google.cloud.bigquery sdk, i could not find it
     https://cloud.google.com/bigquery/docs/reference/rest/v2/TableReference
     {
        "projectId": string,
        "datasetId": string,
        "tableId": string
     }
    """

    def __init__(self, table_uri):
        regexp_str = r'^(?P<project_id>[^.]*)\.(?P<dataset_id>[^.]*)\.(?P<table_id>[^.]*)$'
        reference_regexp = re.compile(regexp_str)
        match = re.match(reference_regexp, table_uri)
        self.table_uri = table_uri
        self.project_id = match.group("project_id")
        self.dataset_id = match.group("dataset_id")
        self.table_id = match.group("table_id")

    def to_dict(self):
        ret = {
            "projectId": self.project_id,
            "datasetId": self.dataset_id,
            "tableId": self.table_id
        }
        return ret


class BigQueryTimePartitioning:
    """
     NOTE: might exist an implementation for this in the google.cloud.bigquery sdk, i could not find it
     https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timepartitioning
     {
        "type": string,
        "expirationMs": string,
        "field": string,
        "requirePartitionFilter": boolean # deprecated
     }
    """

    def __init__(self, type: str, expiration_ms: Optional[str] = None, field: Optional[str] = None):
        if not type in ('DAY', 'HOUR', 'MONTH', 'YEAR'):
            raise ValueError(f"Invalid type: {type}")
        self.type = type
        self.expiration_ms = expiration_ms
        self.field = field

    def to_dict(self) -> dict:
        ret = {
            "type": self.type,
            "expirationMs": self.expiration_ms,
            "field": self.field,
        }
        return ret


class BigQueryJobMixin:
    def query_configuration(
            self, sql_query,
            destination_table: Optional[str] = None,
            create_disposition: Optional[str] = "CREATE_IF_NEEDED",
            write_disposition: Optional[str] = None,
            time_partitioning: Optional[BigQueryTimePartitioning] = None):
        time_partitioning_ref = time_partitioning.to_dict() if time_partitioning else None
        destination_table_ref = TableReference(table_uri=destination_table).to_dict() if destination_table else None
        return {
            "query": {
                "query": sql_query,
                "useLegacySql": False,
                "destinationTable": destination_table_ref,
                "createDisposition": create_disposition,
                "writeDisposition": write_disposition,
                "timePartitioning": time_partitioning_ref
            }
        }

    def extract_configuration(self):
        pass

    def insert_job_operator(self: PMotif, dag, task_group, configuration):
        bigquery_job_operator = BigQueryInsertJobOperator(
            task_id=self.name,
            configuration=configuration,
            dag=dag,
            task_group=task_group
        )
        return bigquery_job_operator
