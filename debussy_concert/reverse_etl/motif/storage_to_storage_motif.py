from airflow.utils.task_group import TaskGroup

from debussy_framework.v3.operators.storage_to_storage import StorageToStorageOperator, StorageHookInterface

from debussy_concert.core.motif.motif_base import MotifBase


class StorageToStorageMotif(MotifBase):
    def __init__(self, *, config,
                 origin_storage_hook: StorageHookInterface,
                 destiny_storage_hook: StorageHookInterface, destiny_file_uri,
                 name=None) -> None:
        super().__init__(config=config, name=name)
        self.origin_storage_hook = origin_storage_hook
        self.destiny_storage_hook = destiny_storage_hook
        self.destiny_file_uri = destiny_file_uri

    def setup(self, storage_uri_prefix):
        self.origin_file_uri = storage_uri_prefix

    def storage_to_storage_operator(self, dag, parent_task_group):
        operator = StorageToStorageOperator(
            task_id=self.name,
            origin_storage_hook=self.origin_storage_hook,
            origin_file_uri=self.origin_file_uri,
            destiny_storage_hook=self.destiny_storage_hook,
            destiny_file_uri=self.destiny_file_uri,
            dag=dag,
            task_group=parent_task_group
        )
        return operator

    def build(self, dag, parent_task_group: TaskGroup):
        return self.storage_to_storage_operator(dag, parent_task_group)
