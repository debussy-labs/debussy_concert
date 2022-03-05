from typing import List, Tuple
from airflow import DAG
from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.config.config_integration import ConfigIntegration


class CompositionBase:
    def __init__(self,
                 config: ConfigIntegration,
                 movements: List[MovementBase]
                 ) -> None:
        self.config = config
        self.movements = movements

    def show(self):
        for movement in self.movements:
            print(movement.name)
            print(', '.join(phrase.name for phrase in movement.phrases))

    def build(self) -> DAG:
        dag = DAG(**self.config.dag_parameters)
        current_task_group = self.movements[0].build(dag)

        for movement in self.movements[1:]:
            movement_task_group = movement.build(dag)
            current_task_group >> movement_task_group
            current_task_group = movement_task_group
        return dag
