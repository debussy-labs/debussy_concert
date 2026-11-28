from typing import Any, Dict, Optional, Sequence

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteClusterOperator,
)


class DebussyDataprocDeleteClusterOperator(DataprocDeleteClusterOperator):
    def execute(self, context: dict):
        from google.api_core.exceptions import NotFound

        try:
            super().execute(context)
        except NotFound:
            self.log.info("Cluster not found. It may already have been deleted.")


class DataprocServerlessSubmitJobOperator(DataprocCreateBatchOperator):
    template_fields: Sequence[str] = (
        "project_id",
        "batch",
        "batch_id",
        "region",
        "impersonation_chain",
    )

    def __init__(
        self,
        region: Optional[str] = None,
        project_id: Optional[str] = None,
        batch: Dict[str, Any] = None,
        batch_id: Optional[str] = None,
        timeout: Optional[float] = None,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(
            region=region,
            project_id=project_id,
            batch=batch,
            batch_id=batch_id,
            timeout=timeout,
            gcp_conn_id=gcp_conn_id,
            **kwargs,
        )

    def execute(self, context):
        DataprocCreateBatchOperator.execute(self, context)
