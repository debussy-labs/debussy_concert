from typing import List
from airflow.utils.task_group import TaskGroup
from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.config.config_integration import ConfigIntegration


class CompositionBase:
    def __init__(
        self,
        config: ConfigIntegration,
        movements: List[MovementBase]
    ) -> None:
        self.name = None
        self.config = config
        self.movements = movements

    def show(self):
        for movement in self.movements:
            print(movement.name)
            print(', '.join(phrase.name for phrase in movement.phrases))

    def build(self, dag) -> TaskGroup:
        task_group = TaskGroup(group_id=self.name, dag=dag)
        current_task_group = self.movements[0].build(dag, task_group)

        for movement in self.movements[1:]:
            movement_task_group = movement.build(dag, task_group)
            current_task_group >> movement_task_group
            current_task_group = movement_task_group
        return task_group

