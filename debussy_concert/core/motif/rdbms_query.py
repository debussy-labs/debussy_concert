from typing import Optional
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.core.motif.mixins.bigquery_job import BigQueryJobMixin, BigQueryTimePartitioning
from debussy_concert.core.phrase.protocols import PExecuteQueryMotif

from debussy_framework.v3.hooks.db_api_hook import DbApiInterface
from debussy_framework.v3.hooks.storage_hook import StorageHookInterface
from debussy_framework.v3.operators.storage_to_rdbms import StorageToRdbmsOperator


class RdbmsQueryMotif(MotifBase, BigQueryJobMixin, PExecuteQueryMotif):
    destination_table = None

    def __init__(self, name=None,
                 write_disposition=None,
                 create_disposition=None,
                 time_partitioning: Optional[BigQueryTimePartitioning] = None,
                 gcp_conn_id='google_cloud_default',
                 **op_kw_args):
        #raise Exception(f"time_partitioning: {time_partitioning}")
        super().__init__(name=name)

        self.write_disposition = write_disposition
        self.time_partitioning = time_partitioning
        self.create_disposition = create_disposition
        self.gcp_conn_id = gcp_conn_id
        self.op_kw_args = op_kw_args

    def setup(self, sql_query,
              destination_table=None):
        self.sql_query = sql_query
        self.destination_table = destination_table
        return self

    def build(self, dag, phrase_group):
        storage_to_rdbms_operator = StorageToRdbmsOperator(dbapi_hook: DbApiInterface,
        storage_hook: StorageHookInterface,
        bucket_file_path)
        '''
        self.insert_job_operator(
            dag, phrase_group,
            self.query_configuration(
                sql_query=self.sql_query,
                destination_table=self.destination_table,
                create_disposition=self.create_disposition,
                write_disposition=self.write_disposition,
                time_partitioning=self.time_partitioning
            ),
            self.gcp_conn_id,
            **self.op_kw_args
        )
        '''
        return storage_to_rdbms_operator
