from typing import TYPE_CHECKING

from debussy.core.motif.motif_base import MotifBase
from debussy.core.tasks import Capability, TaskSpec

if TYPE_CHECKING:
    from debussy.backends.airflow.hooks import DbApiHookInterface, StorageHookInterface


class StorageToRdbmsQueryMotif(MotifBase):
    group_tasks = False
    destination_table = None

    def __init__(
        self,
        dbapi_hook: "DbApiHookInterface",
        storage_hook: "StorageHookInterface",
        destination_table: str,
        name=None,
        **op_kw_args
    ):
        super().__init__(name=name)

        self.dbapi_hook = dbapi_hook
        self.storage_hook = storage_hook
        self.destination_table = destination_table
        self.op_kw_args = op_kw_args

    def setup(self, storage_uri_prefix=None):
        self.storage_file_uri = storage_uri_prefix
        return self

    def tasks(self):
        return [
            TaskSpec(
                name=self.name,
                action="storage.to_rdbms",
                params={
                    "dbapi_hook": self.dbapi_hook,
                    "storage_hook": self.storage_hook,
                    "storage_file_uri": self.storage_file_uri,
                    "table_name": self.destination_table,
                    **self.op_kw_args,
                },
                templated={"storage_file_uri", "table_name"},
                # The hooks are Airflow hooks until connectors are engine-neutral.
                requires={Capability.NATIVE_OPERATOR},
            )
        ]
