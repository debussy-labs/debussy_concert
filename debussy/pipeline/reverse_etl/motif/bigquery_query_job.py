from typing import Optional

from debussy.core.motif.motif_base import MotifBase
from debussy.core.phrase.protocols import PExecuteQueryMotif
from debussy.core.service.lakehouse.bigquery_job import (
    BigQueryJobMixin,
    BigQueryTimePartitioning,
)
from debussy.core.tasks import TaskSpec


class BigQueryQueryJobMotif(MotifBase, BigQueryJobMixin, PExecuteQueryMotif):
    group_tasks = False
    destination_table = None

    def __init__(
        self,
        name=None,
        write_disposition=None,
        create_disposition=None,
        time_partitioning: Optional[BigQueryTimePartitioning] = None,
        gcp_conn_id="google_cloud_default",
        **op_kw_args
    ):
        super().__init__(name=name)

        self.write_disposition = write_disposition
        self.time_partitioning = time_partitioning
        self.create_disposition = create_disposition
        self.gcp_conn_id = gcp_conn_id
        self.op_kw_args = op_kw_args

    def setup(self, sql_query, destination_table=None):
        self.sql_query = sql_query
        self.destination_table = destination_table
        return self

    def tasks(self):
        return [
            TaskSpec(
                name=self.name,
                action="bigquery.insert_job",
                params={
                    "configuration": self.query_configuration(
                        sql_query=self.sql_query,
                        destination_table=self.destination_table,
                        create_disposition=self.create_disposition,
                        write_disposition=self.write_disposition,
                        time_partitioning=self.time_partitioning,
                    ),
                    "gcp_conn_id": self.gcp_conn_id,
                    **self.op_kw_args,
                },
                templated={"configuration"},
            )
        ]
