from abc import abstractclassmethod, ABC
from typing import Callable, List, Protocol, Sequence
import inject

from airflow import DAG

from airflow_concert.movement.movement_base import PMovement
from airflow_concert.config.config_composition import ConfigComposition
from airflow_concert.service.tables.tables import TablesService
from airflow_concert.entities.table import Table
from airflow_concert.service.workflow.protocol import PWorkflowService


class PComposition(Protocol):
    config: ConfigComposition
    tables_service: TablesService
    workflow_service: PWorkflowService

    def play(self, *args, **kwargs) -> DAG:
        pass

    def multi_play(self, *args, **kwargs) -> Sequence[DAG]:
        pass

    def build_multi_dag(self, movement_builder: Callable[[Table], PMovement]) -> Sequence[DAG]:
        pass

    def build(self, movement_builder: Callable[[Table], PMovement]) -> DAG:
        pass


class CompositionBase(ABC, PComposition):
    @inject.autoparams()
    def __init__(
            self, *,
            config: ConfigComposition, workflow_service: PWorkflowService):
        self.config = config
        self.tables_service = TablesService.create_from_dict(config.tables)
        self.workflow_service = workflow_service

    @abstractclassmethod
    def create_from_yaml(cls, environment_config_yaml_filepath, composition_config_yaml_filepath) -> PComposition:
        pass

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def multi_play(self, *args, **kwargs):
        return self.build_multi_dag(*args, **kwargs)

    def build_multi_dag(self, movement_builder: Callable[[Table], PMovement]) -> List[DAG]:
        dags = list()
        for table in self.tables_service.tables():
            name = self.config.dag_parameters.dag_id + '.' + table.name
            kwargs = {**self.config.dag_parameters}
            del kwargs['dag_id']
            workflow_dag = self.workflow_service.workflow_dag(dag_id=name, **kwargs)
            movement_builder(table).play(workflow_dag=workflow_dag)
            dags.append(workflow_dag)
        return dags

    def build(self, movement_builder: Callable[[Table], PMovement]) -> DAG:
        name = self.config.dag_parameters.dag_id
        kwargs = {**self.config.dag_parameters}
        del kwargs['dag_id']
        workflow_dag = self.workflow_service.workflow_dag(dag_id=name, **kwargs)

        for table in self.tables_service.tables():
            movement = movement_builder(table)
            movement.build(workflow_dag=workflow_dag)
        return workflow_dag
