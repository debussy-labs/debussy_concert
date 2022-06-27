from enum import Enum

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.configuration import conf

from debussy_concert.core.service.workflow.protocol import PWorkflowService


class GroupColor(Enum):
    MOVEMENT = '#f7dbdb'
    PHRASE = '#dcedfc'
    MOTIF = '#fff684'


class AirflowService(PWorkflowService):
    @staticmethod
    def motif_group(group_id, workflow_dag, phrase_group: TaskGroup, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            parent_group=phrase_group,
            tooltip=description,
            ui_color=GroupColor.MOTIF.value
        )

    @staticmethod
    def phrase_group(group_id, workflow_dag, movement_group, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            parent_group=movement_group,
            tooltip=description,
            ui_color=GroupColor.PHRASE.value
        )

    @staticmethod
    def movement_group(group_id, workflow_dag, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            tooltip=description,
            ui_color=GroupColor.MOVEMENT.value
        )

    @staticmethod
    def workflow_dag(dag_id, **kwargs):
        return DAG(dag_id=dag_id, **kwargs)

    @staticmethod
    def chain_tasks(*tasks):
        return chain(*tasks)
