from typing import Protocol, Sequence
import inject
from airflow.utils.task_group import TaskGroup
from airflow_concert.phrase.phrase_base import PPhrase
from airflow_concert.service.workflow.protocol import PWorkflowService


class PMovement(Protocol):
    name: str
    phrases: Sequence[PPhrase]
    workflow_service: PWorkflowService

    def play(self, *args, **kwargs):
        pass

    def build(self, dag) -> TaskGroup:
        pass


class MovementBase(PMovement):
    @inject.autoparams()
    def __init__(
        self,
        workflow_service: PWorkflowService,
        phrases: Sequence[PPhrase],
        name=None
    ) -> None:
        self.name = name or self.__class__.__name__
        self.phrases = phrases
        self.workflow_service = workflow_service

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def build(self, workflow_dag) -> TaskGroup:
        movement_group = self.workflow_service.movement_group(group_id=self.name, workflow_dag=workflow_dag)
        current_task_group = self.phrases[0].build(workflow_dag, movement_group)

        for phrase in self.phrases[1:]:
            phrase_task_group = phrase.build(workflow_dag, movement_group)
            current_task_group >> phrase_task_group
            current_task_group = phrase_task_group
        return movement_group
