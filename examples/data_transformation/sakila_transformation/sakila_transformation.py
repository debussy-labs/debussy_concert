import os

from airflow.configuration import conf
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.pipeline.data_transformation.composition.dbt_transformation import (
    DbtTransformationComposition,
)
from debussy_concert.pipeline.data_transformation.config.transform import (
    ConfigTransformComposition,
)

dags_folder = conf.get("core", "dags_folder")
os.environ["DEBUSSY_CONCERT__DAGS_FOLDER"] = dags_folder


workflow_service = AirflowService()

env_file = f"{dags_folder}/examples/environment.yaml"
composition_file = f"{dags_folder}/examples/data_transformation/sakila_transformation/composition.yaml"

config_composition = ConfigTransformComposition.load_from_file(
    composition_config_file_path=composition_file, env_file_path=env_file
)

inject_dependencies(workflow_service, config_composition)

debussy_composition = DbtTransformationComposition()
movement_builder = debussy_composition.dbt_transformation_builder
dag = debussy_composition.build(movement_builder)
