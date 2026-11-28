from typing import Protocol, Sequence

from debussy.core.entities.protocols import (
    PMotifGroup,
    PPhraseGroup,
    PMovementGroup,
    PWorkflowDag,
)
from debussy.core.tasks.spec import TaskSpec


class PWorkflowService(Protocol):
    def motif_group(self, group_id, workflow_dag, phrase_group) -> PMotifGroup:
        pass

    def phrase_group(self, group_id, workflow_dag, movement_group) -> PPhraseGroup:
        pass

    def movement_group(self, group_id, workflow_dag) -> PMovementGroup:
        pass

    def workflow_dag(self, dag_id, **kwargs) -> PWorkflowDag:
        pass

    def chain_tasks(self, *tasks):
        pass

    def build_tasks(
        self,
        group_id: str,
        specs: Sequence[TaskSpec],
        workflow_dag,
        phrase_group,
        grouped: bool = True,
    ):
        """Build a motif's engine-neutral tasks, run in order.

        With ``grouped``, the tasks go in a motif group named ``group_id``,
        which is returned. Without it, there must be exactly one task, built
        directly in ``phrase_group`` and returned.
        """
        pass
