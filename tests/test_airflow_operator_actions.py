"""The Airflow backend's named actions build the operators motifs need."""

import datetime as dt

import inject
import pytest

pytest.importorskip("airflow")

from airflow.sdk import DAG, TaskGroup  # noqa: E402

from debussy.backends.airflow.operator_actions import (  # noqa: E402
    OPERATOR_ACTIONS,
    _import,
)
from debussy.backends.airflow.service import AirflowService  # noqa: E402
from debussy.backends.airflow.tasks import actions, build_task  # noqa: E402
from debussy.core.config.config_composition import ConfigComposition  # noqa: E402
from debussy.core.motif.motif_base import MotifBase  # noqa: E402
from debussy.core.service.workflow.protocol import PWorkflowService  # noqa: E402
from debussy.core.tasks import TaskSpec  # noqa: E402
from debussy.pipeline.data_ingestion.motif.create_external_table import (  # noqa: E402
    CreateExternalBigQueryTableMotif,
)
from debussy.pipeline.data_ingestion.motif.create_update_table_schema import (  # noqa: E402
    CreateBigQueryTableMotif,
)
from debussy.pipeline.data_ingestion.motif.export_table import (  # noqa: E402
    DataprocExportRdbmsTableToGcsMotif,
    DataprocServerlessExportRdbmsTableToGcsMotif,
)
from debussy.pipeline.data_ingestion.motif.load_table import (  # noqa: E402
    LoadGcsToBigQueryHivePartitionMotif,
)
from debussy.pipeline.data_transformation.motif.run_transformation import (  # noqa: E402
    DbtRunMotif,
)
from debussy.pipeline.reverse_etl.motif.bigquery_extract_job import (  # noqa: E402
    BigQueryExtractJobMotif,
)
from debussy.pipeline.reverse_etl.motif.bigquery_query_job import (  # noqa: E402
    BigQueryQueryJobMotif,
)
from debussy.pipeline.reverse_etl.motif.storage_to_rdbms_motif import (  # noqa: E402
    StorageToRdbmsQueryMotif,
)
from debussy.pipeline.reverse_etl.motif.storage_to_storage_motif import (  # noqa: E402
    StorageToStorageMotif,
)

TASK_MOTIFS = [
    BigQueryQueryJobMotif,
    BigQueryExtractJobMotif,
    LoadGcsToBigQueryHivePartitionMotif,
    CreateExternalBigQueryTableMotif,
    CreateBigQueryTableMotif,
    DbtRunMotif,
    StorageToRdbmsQueryMotif,
    StorageToStorageMotif,
    DataprocExportRdbmsTableToGcsMotif,
    DataprocServerlessExportRdbmsTableToGcsMotif,
]


@pytest.mark.parametrize("name", sorted(OPERATOR_ACTIONS))
def test_every_action_is_registered_and_importable(name):
    assert name in actions
    assert isinstance(_import(OPERATOR_ACTIONS[name]), type)


@pytest.fixture
def injected():
    def bindings(binder):
        binder.bind(PWorkflowService, AirflowService())
        binder.bind(ConfigComposition, object())

    inject.clear_and_configure(bindings)
    yield
    inject.clear()


def test_create_external_table_builds_an_external_bigquery_table(injected):
    dag = DAG(dag_id="external", schedule=None, start_date=dt.datetime(2026, 1, 1))
    motif = CreateExternalBigQueryTableMotif(gcp_conn_id="c", name="external")
    motif.setup(
        source_bucket_uri_prefix="gs://raw-vault/sakila/actor",
        destination_project_dataset_table="p.raw_vault.sakila_actor",
    )
    (spec,) = motif.tasks()
    operator = build_task(spec, dag=dag)
    assert type(operator).__name__ == "BigQueryCreateTableOperator"
    assert (operator.project_id, operator.dataset_id, operator.table_id) == (
        "p",
        "raw_vault",
        "sakila_actor",
    )
    assert operator.table_resource["type"] == "EXTERNAL"
    assert operator.table_resource["externalDataConfiguration"]["sourceUris"] == [
        "gs://raw-vault/sakila/actor/*.parquet"
    ]
    assert operator.gcp_conn_id == "c"
    assert operator.if_exists.value == "ignore"


def test_factory_builds_the_operator_with_the_params():
    dag = DAG(dag_id="actions_test", schedule=None, start_date=dt.datetime(2026, 1, 1))
    group = TaskGroup(group_id="Motif", dag=dag)
    spec = TaskSpec(
        name="query",
        action="bigquery.insert_job",
        params={"configuration": {"query": {"query": "select 1"}}, "gcp_conn_id": "c"},
        templated={"configuration"},
        retries=2,
    )
    operator = build_task(spec, dag=dag, task_group=group)
    assert type(operator).__qualname__ == "BigQueryInsertJobOperator"
    assert type(operator).__module__ == "debussy.backends.airflow.bigquery_job"
    assert operator.task_id == "Motif.query"
    assert operator.configuration == {"query": {"query": "select 1"}}
    assert operator.gcp_conn_id == "c"
    assert operator.retries == 2


@pytest.mark.parametrize("motif", TASK_MOTIFS, ids=lambda m: m.__name__)
def test_migrated_motifs_describe_their_work_with_tasks(motif):
    # They must not fall back to building operators themselves.
    assert motif.build is MotifBase.build
    assert callable(getattr(motif, "tasks", None))


def _dag():
    return DAG(dag_id="begin_test", schedule=None, start_date=dt.datetime(2026, 1, 1))


def test_begin_builds_the_start_operator():
    from debussy.backends.airflow.motifs import StartOperator

    spec = TaskSpec(name="begin_film", action="debussy.begin", params={"phase": "film"})
    operator = build_task(spec, dag=_dag())
    assert isinstance(operator, StartOperator)
    assert operator.task_id == "begin_film"


def test_begin_task_name_must_match_the_phase():
    spec = TaskSpec(name="start", action="debussy.begin", params={"phase": "film"})
    with pytest.raises(ValueError, match="begin_film"):
        build_task(spec, dag=_dag())
