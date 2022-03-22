from enum import Enum

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow_concert.service.workflow.protocol import PWorkflowService


class GroupColor(Enum):
    MOVEMENT = '#f7dbdb'
    PHRASE = '#dcedfc'
    MOTIF = '#fff684'


class AirflowService(PWorkflowService):
    def motif_group(self, group_id, workflow_dag, phrase_group: TaskGroup, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            parent_group=phrase_group,
            tooltip=description,
            ui_color=GroupColor.MOTIF.value
        )

    def phrase_group(self, group_id, workflow_dag, movement_group, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            parent_group=movement_group,
            tooltip=description,
            ui_color=GroupColor.PHRASE.value
        )

    def movement_group(self, group_id, workflow_dag, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            tooltip=description,
            ui_color=GroupColor.MOVEMENT.value
        )

    def workflow_dag(self, group_id, **kwargs):
        return DAG(dag_id=group_id, **kwargs)
