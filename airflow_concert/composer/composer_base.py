from typing import Callable

from airflow import DAG

from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.services.tables.tables import TablesService
from airflow_concert.entities.table import Table


class ComposerBase:
    def __init__(self, config: ConfigIntegration):
        self.config = config
        self.tables_service = TablesService.create_from_dict(config.tables)

    @classmethod
    def crate_from_yaml(cls, environment_yaml_filepath, integration_yaml_filepath) -> 'ComposerBase':
        config = ConfigIntegration.load_from_file(
            integration_file_path=integration_yaml_filepath,
            env_file_path=environment_yaml_filepath
        )
        return cls(config)

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def build(self, composition_callable: Callable[[Table], CompositionBase]) -> DAG:
        dag = DAG(**self.config.dag_parameters)

        for table in self.tables_service.tables():
            composition = composition_callable(table)
            composition.build(dag=dag)
        return dag
