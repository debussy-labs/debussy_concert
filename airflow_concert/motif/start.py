import logging

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from airflow_concert.motif.motif_base import MotifBase
from airflow_concert.operators.basic import StartOperator


class StartMotif(MotifBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name, config=config)

    def build(self, dag, parent_task_group):
        task_group = TaskGroup(group_id=self.name, parent_group=parent_task_group)
        start_dag = StartOperator(phase="dag", dag=dag, task_group=task_group)
        log_input = PythonOperator(
            task_id='log_input2',
            python_callable=lambda **x: logging.info(x),
            op_kwargs={'integration': self.config},
            dag=dag,
            task_group=task_group
        )
        start_dag >> log_input
        return task_group
