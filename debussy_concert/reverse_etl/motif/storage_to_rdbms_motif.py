from debussy_concert.core.motif.motif_base import MotifBase
#from debussy_concert.core.motif.mixins.bigquery_job import BigQueryJobMixin
#from debussy_concert.core.phrase.protocols import PExecuteQueryMotif

from debussy_framework.v3.hooks.db_api_hook import DbApiInterface
from debussy_framework.v3.hooks.storage_hook import StorageHookInterface
from debussy_framework.v3.operators.storage_to_rdbms import StorageToRdbmsOperator


class StorageToRdbmsQueryMotif(MotifBase):
    destination_table = None

    def __init__(self, 
                dbapi_hook: DbApiInterface,
                storage_hook: StorageHookInterface,               
                name=None,
                 **op_kw_args):
        super().__init__(name=name)

        self.dbapi_hook = dbapi_hook
        self.storage_hook = storage_hook
        self.op_kw_args = op_kw_args

    def setup(self, bucket_file_path=None,
              destination_table=None):
        self.bucket_file_path = bucket_file_path
        self.destination_table = destination_table
        return self

    def build(self, dag, task_group):
        storage_to_rdbms_operator = StorageToRdbmsOperator(
            task_id=self.name,
            dbapi_hook=self.dbapi_hook,
            storage_hook=self.storage_hook,
            storage_file_uri=self.bucket_file_path,
            table_name = self.destination_table,
            dag=dag, task_group=task_group,
            **self.op_kw_args
            )

        return storage_to_rdbms_operator
