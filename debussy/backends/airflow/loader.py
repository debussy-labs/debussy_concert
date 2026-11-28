"""Generic loader: one file in the DAGs folder builds a DAG for every pipeline.

Pipelines come from two places, and both build the same config objects:

* YAML files under ``pipelines_dir``. A YAML file is a pipeline when it has a
  top-level ``composition_type`` key (see ``COMPOSITION_TYPES``); other YAML
  files, such as table schemas, are ignored.
* Config objects built in Python and passed in ``pipelines``.

Typical DAGs-folder file::

    from debussy.backends.airflow import load_dags

    load_dags(globals(), pipelines_dir="pipelines", environment_file="environment.yaml")

Airflow only parses files that mention both "airflow" and "dag", which the
import above does.
"""

import functools
import logging
import os
import re
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

import inject
from airflow.configuration import conf
from airflow.sdk import DAG

from debussy.backends.airflow.service import AirflowService
from debussy.core.config.config_composition import ConfigComposition
from debussy.core.config.loader_keys import COMPOSITION_TYPE_KEY, ENV_PREFIX_KEY
from debussy.core.service.workflow.protocol import PWorkflowService
from debussy.pipeline.data_ingestion.composition.bigquery_ingestion import (
    BigQueryIngestionComposition,
)
from debussy.pipeline.data_ingestion.composition.rdbms_ingestion import (
    RdbmsIngestionComposition,
)
from debussy.pipeline.data_ingestion.config.bigquery_data_ingestion import (
    ConfigBigQueryDataIngestion,
)
from debussy.pipeline.data_ingestion.config.rdbms_data_ingestion import (
    ConfigRdbmsDataIngestion,
)
from debussy.pipeline.data_transformation.composition.dbt_transformation import (
    DbtTransformationComposition,
)
from debussy.pipeline.data_transformation.config.transform import (
    ConfigTransformComposition,
)
from debussy.pipeline.reverse_etl.composition.bigquery_to_mysql import BigQueryToMysql
from debussy.pipeline.reverse_etl.composition.bigquery_to_storage import (
    ReverseEtlBigQueryToStorageComposition,
)
from debussy.pipeline.reverse_etl.config.reverse_etl import ConfigReverseEtl

log = logging.getLogger(__name__)

# composition_type -> (config class, composition class). A composition class has
# an ``auto_play()`` method that builds its DAG.
COMPOSITION_TYPES: Dict[str, Tuple[type, type]] = {
    "bigquery_ingestion": (ConfigBigQueryDataIngestion, BigQueryIngestionComposition),
    "rdbms_ingestion": (ConfigRdbmsDataIngestion, RdbmsIngestionComposition),
    "dbt_transformation": (ConfigTransformComposition, DbtTransformationComposition),
    "reverse_etl_bigquery_to_mysql": (ConfigReverseEtl, BigQueryToMysql),
    "reverse_etl_bigquery_to_storage": (
        ConfigReverseEtl,
        ReverseEtlBigQueryToStorageComposition,
    ),
}

# A pipeline defined in Python: a config object, or a (config, composition class)
# pair for a composition that is not in COMPOSITION_TYPES.
Pipeline = Union[ConfigComposition, Tuple[ConfigComposition, type]]

YAML_SUFFIXES = (".yaml", ".yml")
_TOP_LEVEL_KEY = r"^{key}\s*:\s*['\"]?([\w.-]+)['\"]?\s*(?:#.*)?$"


def _top_level_value(text: str, key: str) -> Optional[str]:
    match = re.search(_TOP_LEVEL_KEY.format(key=re.escape(key)), text, re.MULTILINE)
    return match.group(1) if match else None


def _resolve(path: Union[str, Path], dags_folder: str) -> Path:
    path = Path(path)
    return path if path.is_absolute() else Path(dags_folder) / path


def _composition_class_for(config: ConfigComposition) -> type:
    for config_cls, composition_cls in COMPOSITION_TYPES.values():
        if type(config) is config_cls:
            return composition_cls
    raise ValueError(
        f"No composition is registered for config {type(config).__name__}; "
        "pass a (config, composition class) pair"
    )


def _python_pipeline(pipeline: Pipeline) -> Tuple[ConfigComposition, type]:
    if isinstance(pipeline, tuple):
        return pipeline
    return pipeline, _composition_class_for(pipeline)


def build_dag(
    config: ConfigComposition,
    composition_cls: type,
    workflow_service: Optional[PWorkflowService] = None,
) -> DAG:
    """Build the DAG of one pipeline. Rebinds the injected dependencies."""
    workflow_service = workflow_service or AirflowService()

    def inject_fn(binder: inject.Binder):
        binder.bind(PWorkflowService, workflow_service)
        binder.bind(ConfigComposition, config)

    inject.clear_and_configure(inject_fn, bind_in_runtime=False)
    return composition_cls().auto_play()


def _yaml_pipeline(
    path: Path, text: str, composition_type: str, environment_file: Path
) -> Tuple[ConfigComposition, type]:
    if composition_type not in COMPOSITION_TYPES:
        raise ValueError(
            f"{path}: unknown {COMPOSITION_TYPE_KEY} {composition_type!r}; "
            f"expected one of {sorted(COMPOSITION_TYPES)}"
        )
    env_prefix = _top_level_value(text, ENV_PREFIX_KEY)
    if env_prefix:
        # The YAML may reference these as ${<prefix>_WINDOW_START}.
        os.environ[f"{env_prefix}_WINDOW_START"] = (
            "logical_date.strftime('%Y-%m-%d 00:00:00')"
        )
        os.environ[f"{env_prefix}_WINDOW_END"] = (
            "data_interval_end.strftime('%Y-%m-%d 00:00:00')"
        )
    config_cls, composition_cls = COMPOSITION_TYPES[composition_type]
    config = config_cls.load_from_file(
        composition_config_file_path=str(path), env_file_path=str(environment_file)
    )
    return config, composition_cls


def _yaml_pipelines(
    pipelines_dir: Path, environment_file: Path
) -> Iterable[Tuple[Path, Callable[[], Tuple[ConfigComposition, type]]]]:
    """Yield each pipeline YAML file with a function that loads its config."""
    for path in sorted(pipelines_dir.rglob("*")):
        if path.suffix not in YAML_SUFFIXES or not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        composition_type = _top_level_value(text, COMPOSITION_TYPE_KEY)
        if composition_type is None:
            continue
        yield path, functools.partial(
            _yaml_pipeline, path, text, composition_type, environment_file
        )


def load_dags(
    namespace: MutableMapping,
    *,
    pipelines_dir: Union[str, Path, None] = None,
    environment_file: Union[str, Path, None] = None,
    pipelines: Iterable[Pipeline] = (),
    workflow_service: Optional[PWorkflowService] = None,
    strict: bool = False,
) -> List[DAG]:
    """Build one DAG per pipeline and add each to ``namespace`` by its dag id.

    A pipeline that cannot be built is logged and skipped, so that one broken
    pipeline does not hide the DAGs of all the others: Airflow drops every DAG
    of a file whose parsing raises. Pass ``strict=True`` to raise instead, for
    example in tests or CI.

    :param namespace: the module's ``globals()``; Airflow finds DAGs there.
    :param pipelines_dir: folder searched recursively for pipeline YAML files.
        Relative paths are relative to Airflow's ``dags_folder``.
    :param environment_file: environment YAML shared by the YAML pipelines.
    :param pipelines: pipelines defined in Python.
    :param workflow_service: defaults to ``AirflowService``.
    :param strict: raise on the first pipeline that fails to build.
    """
    dags_folder = conf.get("core", "dags_folder")
    os.environ["DEBUSSY__DAGS_FOLDER"] = dags_folder

    # (source, function returning the config and the composition class)
    found: List[Tuple[str, Callable[[], Tuple[ConfigComposition, type]]]] = []
    if pipelines_dir is not None:
        if environment_file is None:
            raise ValueError("environment_file is required with pipelines_dir")
        found.extend(
            (str(path), load)
            for path, load in _yaml_pipelines(
                _resolve(pipelines_dir, dags_folder),
                _resolve(environment_file, dags_folder),
            )
        )
    for pipeline in pipelines:
        found.append(("<python>", functools.partial(_python_pipeline, pipeline)))

    dags: List[DAG] = []
    sources: Dict[str, str] = {}
    failed: List[str] = []
    for source, load in found:
        try:
            config, composition_cls = load()
            dag_id = config.dag_parameters.dag_id
            if dag_id in sources:
                raise ValueError(
                    f"Duplicate dag id {dag_id!r}: "
                    f"defined in {sources[dag_id]} and {source}"
                )
            sources[dag_id] = source
            dag = build_dag(config, composition_cls, workflow_service)
        except Exception:
            if strict:
                raise
            log.exception("Debussy could not build the pipeline from %s", source)
            failed.append(source)
            continue
        namespace[dag.dag_id] = dag
        dags.append(dag)
    if failed:
        log.error(
            "Debussy skipped %d pipeline(s) that failed to build: %s",
            len(failed),
            ", ".join(failed),
        )
    return dags
