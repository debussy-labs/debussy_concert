"""Airflow implementations of the motifs every pipeline starts and ends with."""

import logging
from datetime import datetime, timezone

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator, TaskGroup

from debussy.core.motif.motif_base import MotifBase


class StartOperator(BaseOperator):
    """First task of a phase: returns the run's dates as its XCom value."""

    def __init__(self, phase, **kwargs):
        super().__init__(task_id=f"begin_{phase}", **kwargs)
        self.phase = phase

    def execute(self, context):
        def isoformat(value):
            return value.isoformat() if value else None

        return {
            "logical_date": isoformat(context.get("logical_date")),
            "data_interval_start": isoformat(context.get("data_interval_start")),
            "data_interval_end": isoformat(context.get("data_interval_end")),
            "current_date": datetime.now(timezone.utc).isoformat(),
        }


class StartMotif(MotifBase):
    def __init__(self, name=None) -> None:
        super().__init__(name=name)

    def build(self, dag, parent_task_group):
        task_group = TaskGroup(
            group_id=self.name, dag=dag, parent_group=parent_task_group
        )
        start_dag = StartOperator(phase="dag", dag=dag, task_group=task_group)
        log_input = PythonOperator(
            task_id="log_input",
            python_callable=lambda **x: logging.info(x),
            op_kwargs={"config": self.config},
            dag=dag,
            task_group=task_group,
        )
        self.workflow_service.chain_tasks(start_dag, log_input)
        return task_group


class EndMotif(MotifBase):
    def __init__(self, name=None) -> None:
        super().__init__(name=name)

    def build(self, dag, phrase_group):
        operator = EmptyOperator(task_id=self.name, dag=dag, task_group=phrase_group)
        return operator


class DummyMotif(MotifBase):
    def __init__(self, name=None) -> None:
        super().__init__(name=name)

    def build(self, workflow_dag, phrase_group):
        operator = EmptyOperator(
            task_id=self.name, dag=workflow_dag, task_group=phrase_group
        )
        return operator

    def setup(self, *args, **kwargs):
        pass
