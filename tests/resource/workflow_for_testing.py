from debussy_concert.core.service.workflow.protocol import PWorkflowService
from .core_for_testing import (
    DummyDag,
    DummyMovementGroup,
    DummyPhraseGroup,
    DummyMotifGroup,
)


class WorkflowServiceForTesting(PWorkflowService):
    def motif_group(self, group_id, workflow_dag, phrase_group) -> DummyMotifGroup:
        return DummyMotifGroup(
            group_id=group_id, workflow_dag=workflow_dag, phrase_group=phrase_group
        )

    def phrase_group(self, group_id, workflow_dag, movement_group) -> DummyPhraseGroup:
        return DummyPhraseGroup(
            group_id=group_id, workflow_dag=workflow_dag, movement_group=movement_group
        )

    def movement_group(self, group_id, workflow_dag) -> DummyMovementGroup:
        return DummyMovementGroup(group_id=group_id, workflow_dag=workflow_dag)

    def workflow_dag(self, dag_id, **kwargs) -> DummyDag:
        return DummyDag(dag_id=dag_id, **kwargs)
