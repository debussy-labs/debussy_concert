import os

from airflow.configuration import conf
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.core.service.injection import inject_dependencies


def airflow_easy_setup(
    rel_path_env_file,
    rel_path_composition_file,
    CompositionCls,
    CompositionConfigCls,
    os_env_prefix=None
):
    dags_folder = conf.get("core", "dags_folder")
    env_file = f"{dags_folder}/{rel_path_env_file}"
    composition_file = f"{dags_folder}/{rel_path_composition_file}"

    os.environ["DEBUSSY_CONCERT__DAGS_FOLDER"] = dags_folder
    if os_env_prefix:
        os.environ[f"{os_env_prefix}_WINDOW_START"] = "execution_date.strftime('%Y-%m-%d 00:00:00')"
        os.environ[f"{os_env_prefix}_WINDOW_END"] = "next_execution_date.strftime('%Y-%m-%d 00:00:00')"

    workflow_service = AirflowService()
    config_composition = CompositionConfigCls['config'].load_from_file(
        composition_config_file_path=composition_file,
        env_file_path=env_file
    )

    inject_dependencies(workflow_service, config_composition)

    debussy_composition = CompositionCls['composition']()
    return debussy_composition
