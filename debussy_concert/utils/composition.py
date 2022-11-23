import os

from airflow.configuration import conf
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.pipeline.data_ingestion.config.bigquery_data_ingestion import (
    ConfigBigQueryDataIngestion,
)
from debussy_concert.pipeline.data_ingestion.composition.bigquery_ingestion import (
    BigQueryIngestionComposition,
)
from debussy_concert.pipeline.data_ingestion.config.rdbms_data_ingestion import (
    ConfigRdbmsDataIngestion,
)
from debussy_concert.pipeline.data_ingestion.composition.rdbms_ingestion import (
    RdbmsIngestionComposition,
)
from debussy_concert.pipeline.data_transformation.config.transform import (
    ConfigTransformComposition,
)
from debussy_concert.pipeline.data_transformation.composition.dbt_transformation import (
    DbtTransformationComposition,
)
from debussy_concert.pipeline.reverse_etl.config.reverse_etl import ConfigReverseEtl
from debussy_concert.pipeline.reverse_etl.composition.bigquery_to_mysql import (
    BigQueryToMysql,
)
from debussy_concert.pipeline.reverse_etl.composition.bigquery_to_storage import (
    ReverseEtlBigQueryToStorageComposition,
)


def airflow_easy_setup(
    rel_path_env_file,
    rel_path_composition_file,
    CompositionCls,
    CompositionConfigCls,
    composition_type=None,
    os_env_prefix=None,
):
    dags_folder = conf.get("core", "dags_folder")
    env_file = f"{dags_folder}/{rel_path_env_file}"
    composition_file = f"{dags_folder}/{rel_path_composition_file}"

    _cls_map = {
        "bigquery_ingestion": {
            "config": ConfigBigQueryDataIngestion,
            "composition": BigQueryIngestionComposition,
        },
        "rdbms_ingestion": {
            "config": ConfigRdbmsDataIngestion,
            "composition": RdbmsIngestionComposition,
        },
        "dbt_transformation": {
            "config": ConfigTransformComposition,
            "composition": DbtTransformationComposition,
        },
        "reverse_etl_bigquery_to_mysql": {
            "config": ConfigReverseEtl,
            "composition": BigQueryToMysql,
        },
        "reverse_etl_bigquery_to_storage": {
            "config": ConfigReverseEtl,
            "composition": ReverseEtlBigQueryToStorageComposition,
        },
    }

    if composition_type:
        CompositionConfigCls = _cls_map[composition_type]["config"]()
        CompositionCls = _cls_map[composition_type]["composition"]()

    os.environ["DEBUSSY_CONCERT__DAGS_FOLDER"] = dags_folder
    if os_env_prefix:
        os.environ[
            f"{os_env_prefix}_WINDOW_START"
        ] = "execution_date.strftime('%Y-%m-%d 00:00:00')"
        os.environ[
            f"{os_env_prefix}_WINDOW_END"
        ] = "next_execution_date.strftime('%Y-%m-%d 00:00:00')"

    workflow_service = AirflowService()
    config_composition = CompositionConfigCls["config"].load_from_file(
        composition_config_file_path=composition_file, env_file_path=env_file
    )

    inject_dependencies(workflow_service, config_composition)

    debussy_composition = CompositionCls["composition"]()
    return debussy_composition
