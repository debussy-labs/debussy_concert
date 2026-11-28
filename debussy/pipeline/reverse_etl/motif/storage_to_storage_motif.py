from typing import TYPE_CHECKING

from debussy.core.motif.motif_base import MotifBase
from debussy.core.tasks import Capability, TaskSpec

if TYPE_CHECKING:
    from debussy.backends.airflow.hooks import StorageHookInterface


class StorageToStorageMotif(MotifBase):
    group_tasks = False

    def __init__(
        self,
        *,
        origin_storage_hook: "StorageHookInterface",
        destiny_storage_hook: "StorageHookInterface",
        destiny_file_uri,
        name=None
    ) -> None:
        super().__init__(name=name)
        self.origin_storage_hook = origin_storage_hook
        self.destiny_storage_hook = destiny_storage_hook
        self.destiny_file_uri = destiny_file_uri

    def setup(self, storage_uri_prefix):
        self.origin_file_uri = storage_uri_prefix

    def tasks(self):
        return [
            TaskSpec(
                name=self.name,
                action="storage.to_storage",
                params={
                    "origin_storage_hook": self.origin_storage_hook,
                    "origin_file_uri": self.origin_file_uri,
                    "destination_storage_hook": self.destiny_storage_hook,
                    "destination_file_uri": self.destiny_file_uri,
                },
                templated={"origin_file_uri", "destination_file_uri"},
                # The hooks are Airflow hooks until connectors are engine-neutral.
                requires={Capability.NATIVE_OPERATOR},
            )
        ]
