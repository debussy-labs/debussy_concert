from typing import Protocol

from airflow_concert.entities.protocols import (
    PMotifGroup, PPhraseGroup, PMovementGroup, PWorkflowDag)


class PWorkflowService(Protocol):
    def motif_group(self, group_id, workflow_dag, phrase_group) -> PMotifGroup:
        pass

    def phrase_group(self, group_id, workflow_dag, movement_group) -> PPhraseGroup:
        pass

    def movement_group(self, group_id, workflow_dag) -> PMovementGroup:
        pass

    def workflow_dag(self, group_id, **kwargs) -> PWorkflowDag:
        pass
