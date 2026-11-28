"""Operator factories for the named actions motifs use.

Each action builds the same operator the motif built before it described its
work as ``TaskSpec`` objects. Operator classes are imported when an action is
built, so the provider packages (for example Google Cloud) are only needed by
pipelines that use them.
"""

from importlib import import_module

from debussy.backends.airflow.tasks import actions

#: Action name -> "module:OperatorClass". The spec's params are passed to the
#: operator as keyword arguments.
OPERATOR_ACTIONS = {
    "bigquery.insert_job": "debussy.backends.airflow.bigquery_job:BigQueryInsertJobOperator",
    "bigquery.create_table": (
        "airflow.providers.google.cloud.operators.bigquery:BigQueryCreateTableOperator"
    ),
    "bigquery.update_table": (
        "airflow.providers.google.cloud.operators.bigquery:BigQueryUpdateTableOperator"
    ),
    "dataproc.create_cluster": (
        "airflow.providers.google.cloud.operators.dataproc"
        ":DataprocCreateClusterOperator"
    ),
    "dataproc.submit_job": (
        "airflow.providers.google.cloud.operators.dataproc:DataprocSubmitJobOperator"
    ),
    "dataproc.delete_cluster": (
        "debussy.backends.airflow.dataproc:DebussyDataprocDeleteClusterOperator"
    ),
    "dataproc.create_batch": (
        "debussy.backends.airflow.dataproc:DataprocServerlessSubmitJobOperator"
    ),
    "debussy.noop": "airflow.providers.standard.operators.empty:EmptyOperator",
    "dbt.run": "debussy.backends.airflow.dbt:DebussyDbtRunOperator",
    "storage.to_rdbms": "debussy.backends.airflow.operators:StorageToRdbmsOperator",
    "storage.to_storage": "debussy.backends.airflow.operators:StorageToStorageOperator",
}


def _import(path: str):
    module_name, _, attribute = path.partition(":")
    return getattr(import_module(module_name), attribute)


def operator_factory(path: str):
    """A factory that builds the operator at ``path`` ("module:Class") named
    after the spec, with the spec's params as keyword arguments."""

    def build(spec, *, dag, task_group, **operator_kwargs):
        operator_class = _import(path)
        return operator_class(
            task_id=spec.name,
            dag=dag,
            task_group=task_group,
            **spec.params,
            **operator_kwargs,
        )

    build.operator_path = path
    return build


for _name, _path in OPERATOR_ACTIONS.items():
    actions.register(_name, operator_factory(_path))


@actions.register("debussy.begin")
def begin(spec, *, dag, task_group, **operator_kwargs):
    """The first task of a phase (``StartOperator``), which returns the run's
    dates. Its name must be ``begin_<phase>``."""
    from debussy.backends.airflow.motifs import StartOperator

    phase = spec.params["phase"]
    if spec.name != f"begin_{phase}":
        raise ValueError(f"Task {spec.name!r}: a debussy.begin task is begin_{phase}")
    return StartOperator(phase=phase, dag=dag, task_group=task_group, **operator_kwargs)
