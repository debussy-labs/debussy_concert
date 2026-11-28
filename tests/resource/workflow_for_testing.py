from debussy.core.service.workflow.protocol import PWorkflowService
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

    def chain_tasks(self, *tasks):
        for upstream, downstream in zip(tasks, tasks[1:]):
            upstream >> downstream

    def build_tasks(self, group_id, specs, workflow_dag, phrase_group, grouped=True):
        # Records what a backend would build, so core tests can check it.
        group = self.motif_group(group_id, workflow_dag, phrase_group)
        group.specs = tuple(specs)
        group.grouped = grouped
        return group
