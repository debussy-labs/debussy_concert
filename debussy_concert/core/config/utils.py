import os
from airflow.configuration import conf
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService


def composition_setup(rel_path_env_file, rel_path_composition_file, CompositionCls, CompositionConfigCls):
    dags_folder = conf.get("core", "dags_folder")
    env_file = f"{dags_folder}/{rel_path_env_file}"
    composition_file = f"{dags_folder}/{rel_path_composition_file}"

    os.environ[
        "DEBUSSY_CONCERT__DAGS_FOLDER"
    ] = dags_folder

    workflow_service = AirflowService()
    config_composition = CompositionConfigCls['config'].load_from_file(
        composition_config_file_path=composition_file, env_file_path=env_file
    )

    inject_dependencies(workflow_service, config_composition)

    debussy_composition = CompositionCls['composition']()
    return debussy_composition
