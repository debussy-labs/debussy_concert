from enum import Enum

from airflow.sdk import DAG, TaskGroup, chain

from debussy.backends.airflow.tasks import build_task
from debussy.core.service.workflow.protocol import PWorkflowService
from debussy.core.tasks import validate_tasks


class GroupColor(Enum):
    MOVEMENT = "#f7dbdb"
    PHRASE = "#dcedfc"
    MOTIF = "#fff684"


class AirflowService(PWorkflowService):
    @staticmethod
    def motif_group(group_id, workflow_dag, phrase_group: TaskGroup, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            parent_group=phrase_group,
            tooltip=description,
            ui_color=GroupColor.MOTIF.value,
        )

    @staticmethod
    def phrase_group(group_id, workflow_dag, movement_group, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            parent_group=movement_group,
            tooltip=description,
            ui_color=GroupColor.PHRASE.value,
        )

    @staticmethod
    def movement_group(group_id, workflow_dag, description=""):
        return TaskGroup(
            group_id=group_id,
            dag=workflow_dag,
            tooltip=description,
            ui_color=GroupColor.MOVEMENT.value,
        )

    @staticmethod
    def workflow_dag(dag_id, **kwargs):
        return DAG(dag_id=dag_id, **kwargs)

    @staticmethod
    def chain_tasks(*tasks):
        return chain(*tasks)

    @staticmethod
    def build_tasks(group_id, specs, workflow_dag, phrase_group, grouped=True):
        specs = validate_tasks(specs)
        if not specs:
            raise ValueError(f"Motif {group_id!r} has no tasks")
        if grouped:
            parent = AirflowService.motif_group(
                group_id=group_id, workflow_dag=workflow_dag, phrase_group=phrase_group
            )
        elif len(specs) == 1:
            parent = phrase_group
        else:
            raise ValueError(
                f"Motif {group_id!r} has {len(specs)} tasks: only a motif with"
                " one task can build it outside a motif group"
            )
        tasks = [
            build_task(spec, dag=workflow_dag, task_group=parent) for spec in specs
        ]
        if len(tasks) > 1:
            chain(*tasks)
        return parent if grouped else tasks[0]
