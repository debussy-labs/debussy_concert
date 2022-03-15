# -*- coding: utf-8 -*-

import json
import enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
)

from google.cloud.bigquery import TableReference  # type: ignore
from google.api_core.exceptions import Conflict  # type: ignore

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url

from airflow.models import BaseOperator


from debussy_framework.v2.utils.bigquery import bigquery_singlevalue_formatter  # type: ignore


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators"""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"


class BigQueryGetDataOperator(BaseOperator):
    ui_color = BigQueryUIColors.QUERY.value
    template_fields = [
        "sql", 'sql_template_params'
    ]

    def __init__(
        self,
        sql="",
        sql_template_params={},
        bigquery_conn_id="bigquery_default",
        delegate_to=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.sql_template_params = sql_template_params
        self.sql = sql

    def execute(self, context):
        try:
            self.sql = self.SQL_TEMPLATE.format(**self.sql_template_params)
        except AttributeError:
            self.sql = self.sql.format(**self.sql_template_params)

        bq_hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
            use_legacy_sql=False,
        )
        bq_cursor = bq_hook.get_conn().cursor()
        print(f"EXECUTING QUERY: {self.sql}")
        bq_cursor.execute(self.sql)
        result = bq_cursor.fetchall()

        return result


class BigQueryGetSingleValueOperator(BigQueryGetDataOperator):
    SQL_TEMPLATE = """
SELECT
    {max_field} AS value
FROM
    `{project_id}.{dataset_id}.{table_id}`
WHERE
    {where_clause}
    """

    def __init__(
        self,
        project_id,
        dataset_id,
        table_id,
        field_id,
        field_type,
        aggregation_function,
        where_clause=None,
        format_string=None,
        timezone=None,
        bigquery_conn_id="bigquery_default",
        delegate_to=None,
        **kwargs,
    ):
        max_field = bigquery_singlevalue_formatter(
            aggregation_function=aggregation_function,
            field_id=field_id,
            field_type=field_type,
            format_string=format_string,
            timezone=timezone,
        )

        if not where_clause:
            where_clause = "True"

        sql_template_params = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "max_field": max_field,
            "where_clause": where_clause,
        }
        super().__init__(sql_template_params=sql_template_params, **kwargs)

    def execute(self, context):
        data = super().execute(context)

        # getting the 1st cell of the 1st row of the resultset
        return data[0][0]


class BigQueryGetMaxFieldOperator(BigQueryGetSingleValueOperator):
    def __init__(self, **kwargs):
        super().__init__(
            aggregation_function="MAX",
            **kwargs,
        )

    def execute(self, context):
        return super().execute(context)


class BigQueryGetMinFieldOperator(BigQueryGetSingleValueOperator):
    def __init__(self, **kwargs):
        super().__init__(
            aggregation_function="MIN",
            **kwargs,
        )

    def execute(self, context):
        return super().execute(context)


# TODO: copied from airflow source code to fix some bugs, should de removed in the future
class BigQueryCreateEmptyTableOperator(BaseOperator):
    template_fields = (
        "dataset_id",
        "table_id",
        "project_id",
        "gcs_schema_object",
        "labels",
        "view",
        "materialized_view",
        "impersonation_chain",
    )
    template_fields_renderers = {"table_resource": "json", "materialized_view": "json"}
    ui_color = BigQueryUIColors.TABLE.value

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        table_resource: Optional[Dict[str, Any]] = None,
        project_id: Optional[str] = None,
        gcs_schema_object: Optional[str] = None,
        time_partitioning: Optional[Dict] = None,
        bigquery_conn_id: str = "google_cloud_default",
        google_cloud_storage_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        labels: Optional[Dict] = None,
        view: Optional[Dict] = None,
        materialized_view: Optional[Dict] = None,
        encryption_configuration: Optional[Dict] = None,
        location: Optional[str] = None,
        cluster_fields: Optional[List[str]] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        exists_ok: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcs_schema_object = gcs_schema_object
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = {} if time_partitioning is None else time_partitioning
        self.labels = labels
        self.view = view
        self.materialized_view = materialized_view
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.cluster_fields = cluster_fields
        self.table_resource = table_resource
        self.impersonation_chain = impersonation_chain
        self.exists_ok = exists_ok

    def execute(self, context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if self.gcs_schema_object:
            gcs_bucket, gcs_object = _parse_gcs_url(self.gcs_schema_object)
            gcs_hook = GCSHook(
                gcp_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
            schema_fields = json.loads(gcs_hook.download(gcs_object, gcs_bucket))
        else:
            schema_fields = None

        try:
            self.log.info("Creating table")
            table = (
                bq_hook.create_empty_table(  # pylint: disable=unexpected-keyword-arg
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_id=self.table_id,
                    schema_fields=schema_fields,
                    time_partitioning=self.time_partitioning,
                    cluster_fields=self.cluster_fields,
                    labels=self.labels,
                    view=self.view,
                    materialized_view=self.materialized_view,
                    encryption_configuration=self.encryption_configuration,
                    table_resource=self.table_resource,
                    exists_ok=self.exists_ok,
                )
            )
            self.log.info(
                "Table %s.%s.%s created successfully",
                table.project,
                table.dataset_id,
                table.table_id,
            )
        except Conflict:
            self.log.info("Table %s.%s already exists.", self.dataset_id, self.table_id)


class BigQueryCreateExternalTableOperator(BaseOperator):
    template_fields = (
        "bucket",
        "schema_object",
        "table_resource",
        "impersonation_chain",
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value

    # pylint: disable=too-many-arguments,too-many-locals
    def __init__(
        self,
        *,
        bucket: str,
        destination_project_dataset_table: str,
        table_resource: Dict[str, Any],
        schema_object: Optional[str] = None,
        bigquery_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        encryption_configuration: Optional[Dict] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.bucket = bucket
        self.schema_object = schema_object
        self.table_resource = table_resource
        self.destination_project_dataset_table = destination_project_dataset_table
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if self.schema_object:
            gcs_hook = GCSHook(
                gcp_conn_id=self.google_cloud_storage_conn_id,  # pylint: disable=no-member
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
            schema_fields = json.loads(
                gcs_hook.download(self.bucket, self.schema_object)
            )
        else:
            schema_fields = None

        if schema_fields and self.table_resource:
            self.table_resource["externalDataConfiguration"]["schema"] = schema_fields

        tab_ref = TableReference.from_string(self.destination_project_dataset_table)
        bq_hook.create_empty_table(
            table_resource=self.table_resource,
            project_id=tab_ref.project,
            table_id=tab_ref.table_id,
            dataset_id=tab_ref.dataset_id,
        )
