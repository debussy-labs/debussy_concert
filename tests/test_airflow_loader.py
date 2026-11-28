"""``load_dags``: YAML and Python pipelines build the same DAGs."""

import copy
import datetime as dt

import inject
import pytest

pytest.importorskip("airflow")

import yaml  # noqa: E402
from airflow.configuration import conf  # noqa: E402

from debussy.backends.airflow import load_dags  # noqa: E402
from debussy.core.config.config_dag_parameters import ConfigDagParameters  # noqa: E402
from debussy.core.config.config_environment import ConfigEnvironment  # noqa: E402
from debussy.pipeline.data_ingestion.config.movement_parameters.rdbms_data_ingestion import (  # noqa: E402
    RdbmsDataIngestionMovementParameters,
)
from debussy.pipeline.data_ingestion.config.movement_parameters.time_partitioned import (  # noqa: E402
    BigQueryDataPartitioning,
)
from debussy.pipeline.data_ingestion.config.rdbms_data_ingestion import (  # noqa: E402
    ConfigRdbmsDataIngestion,
)
from tests.characterization.dag_snapshot import dag_to_snapshot, dump  # noqa: E402

ENVIRONMENT = {
    "project": "p",
    "region": "r",
    "zone": "z",
    "artifact_bucket": "artifacts",
    "staging_bucket": "staging",
    "landing_bucket": "landing",
    "raw_vault_bucket": "raw-vault-bucket",
    "reverse_etl_bucket": "reverse-etl-bucket",
    "raw_vault_dataset": "raw_vault",
    "raw_dataset": "raw",
    "trusted_dataset": "trusted",
    "reverse_etl_dataset": "reverse_etl",
    "temp_dataset": "temp",
    "data_lakehouse_connection_id": "google_cloud_debussy",
}
TABLE = {
    "fields": [
        {"name": "id", "data_type": "INT64", "description": "key"},
        {"name": "last_update", "data_type": "TIMESTAMP", "description": "updated"},
    ],
    "partitioning": {"field": "last_update", "type": "time", "granularity": "DAY"},
}
DAG = {
    "dag_id": "twin",
    "description": "twin",
    "catchup": False,
    "schedule": "@daily",
    "max_active_runs": 1,
    "start_date": {"year": 2006, "month": 2, "day": 15},
    "tags": ["a", "b"],
    "default_args": {"owner": "debussy"},
}
DATAPROC = {
    "machine_type": "n1-standard-2",
    "num_workers": 0,
    "parallelism": 4,
    "subnet": "sn",
}
PARTITIONING = {
    "gcs_partition_schema": "_load_flag=incr/_ts={{ logical_date }}",
    "destination_partition": "{{ logical_date.strftime('%Y%m%d') }}",
}
QUERY = "SELECT id, last_update FROM sakila.t"


@pytest.fixture(autouse=True)
def airflow_conf(tmp_path, monkeypatch):
    previous = conf.get("core", "dags_folder")
    conf.set("core", "dags_folder", str(tmp_path))
    monkeypatch.setenv("AIRFLOW_CONN_GOOGLE_CLOUD_DEBUSSY", "google-cloud-platform://")
    inject.clear()
    yield tmp_path
    inject.clear()
    conf.set("core", "dags_folder", previous)


def _write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def _yaml_pipeline(root, name="twin", composition_type="rdbms_ingestion"):
    _write(root / "environment.yaml", ENVIRONMENT)
    _write(root / "pipelines" / "schemas" / "t.yaml", TABLE)
    _write(
        root / "pipelines" / f"{name}.yaml",
        {
            "composition_type": composition_type,
            "name": name,
            "source_name": "sakila",
            "source_type": "mysql",
            "description": "twin",
            "secret_manager_uri": "projects/p/secrets/s",
            "dataproc_config": DATAPROC,
            "dag_parameters": {**DAG, "dag_id": name},
            "ingestion_parameters": [
                {
                    "name": "t",
                    "extraction_query": QUERY,
                    "extract_connection_id": "google_cloud_debussy",
                    "raw_table_definition": str(root / "pipelines/schemas/t.yaml"),
                    "data_partitioning": PARTITIONING,
                }
            ],
        },
    )


def _python_config(name="twin"):
    return ConfigRdbmsDataIngestion(
        name=name,
        description="twin",
        source_type="mysql",
        source_name="sakila",
        secret_manager_uri="projects/p/secrets/s",
        dataproc_config=dict(DATAPROC),
        environment=ConfigEnvironment(**ENVIRONMENT),
        dag_parameters=ConfigDagParameters(
            dag_id=name,
            description="twin",
            catchup=False,
            schedule="@daily",
            max_active_runs=1,
            start_date=dt.datetime(2006, 2, 15),
            tags=["a", "b"],
            default_args={"owner": "debussy"},
        ),
        movements_parameters=[
            RdbmsDataIngestionMovementParameters(
                name="t",
                extract_connection_id="google_cloud_debussy",
                data_partitioning=BigQueryDataPartitioning(**PARTITIONING),
                raw_table_definition=copy.deepcopy(TABLE),
                extraction_query=QUERY,
            )
        ],
    )


def test_yaml_and_python_pipelines_build_the_same_dag(airflow_conf):
    _yaml_pipeline(airflow_conf)
    from_yaml = {}
    load_dags(from_yaml, pipelines_dir="pipelines", environment_file="environment.yaml")
    from_python = {}
    load_dags(from_python, pipelines=[_python_config()])

    assert list(from_yaml) == list(from_python) == ["twin"]
    replacements = [(str(airflow_conf), "<DAGS_FOLDER>")]
    assert dump(dag_to_snapshot(from_yaml["twin"], replacements)) == dump(
        dag_to_snapshot(from_python["twin"], replacements)
    )


def test_yaml_files_without_composition_type_are_ignored(airflow_conf):
    _yaml_pipeline(airflow_conf)
    namespace = {}
    load_dags(namespace, pipelines_dir="pipelines", environment_file="environment.yaml")
    assert list(namespace) == ["twin"]  # the table schema YAML was not a pipeline


def test_duplicate_dag_id_is_an_error(airflow_conf):
    _yaml_pipeline(airflow_conf)
    with pytest.raises(ValueError, match="Duplicate dag id 'twin'"):
        load_dags(
            {},
            pipelines_dir="pipelines",
            environment_file="environment.yaml",
            pipelines=[_python_config()],
            strict=True,
        )


def test_duplicate_dag_id_keeps_the_first_pipeline(airflow_conf, caplog):
    _yaml_pipeline(airflow_conf)
    namespace = {}
    dags = load_dags(
        namespace,
        pipelines_dir="pipelines",
        environment_file="environment.yaml",
        pipelines=[_python_config()],
    )
    assert list(namespace) == ["twin"] and len(dags) == 1
    assert "Duplicate dag id 'twin'" in caplog.text


def test_unknown_composition_type_is_an_error(airflow_conf):
    _yaml_pipeline(airflow_conf, composition_type="nope")
    with pytest.raises(ValueError, match="unknown composition_type 'nope'"):
        load_dags(
            {},
            pipelines_dir="pipelines",
            environment_file="environment.yaml",
            strict=True,
        )


def test_a_broken_pipeline_does_not_hide_the_others(airflow_conf, caplog):
    _yaml_pipeline(airflow_conf, composition_type="nope")
    namespace = {}
    load_dags(
        namespace,
        pipelines_dir="pipelines",
        environment_file="environment.yaml",
        pipelines=[_python_config("other")],
    )
    assert list(namespace) == ["other"]
    assert "unknown composition_type 'nope'" in caplog.text
    assert "skipped 1 pipeline(s)" in caplog.text


def test_pipelines_dir_needs_an_environment_file():
    with pytest.raises(ValueError, match="environment_file"):
        load_dags({}, pipelines_dir="pipelines")
