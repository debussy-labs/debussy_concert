from typing import Protocol


class PWorkflowService(Protocol):
    def motif_group(self, group_id, workflow_dag, phrase_group):
        pass

    def phrase_group(self, group_id, workflow_dag, movement_group):
        pass

    def movement_group(self, group_id, workflow_dag):
        pass

    def workflow_dag(self, group_id, **kwargs):
        pass
