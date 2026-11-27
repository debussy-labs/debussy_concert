"""Characterization tests: every example DAG must keep generating the same graph.

Each file under ``examples/`` is executed the way the Airflow scheduler would
parse it (with ``AirflowService``), and the resulting DAG is compared with a
committed JSON snapshot in ``tests/characterization/snapshots/``.

When a change to the generated DAGs is intended, regenerate the snapshots and
review the diff like any other code change::

    DEBUSSY_UPDATE_SNAPSHOTS=1 pytest tests/characterization
"""
import os
import runpy
from pathlib import Path

import inject
import pytest

pytest.importorskip("airflow")

from airflow.configuration import conf  # noqa: E402
from airflow.models.dag import DAG  # noqa: E402

from tests.characterization.dag_snapshot import dag_to_snapshot, dump  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = REPO_ROOT / "examples"
SNAPSHOTS_DIR = Path(__file__).resolve().parent / "snapshots"
UPDATE = os.environ.get("DEBUSSY_UPDATE_SNAPSHOTS", "").lower() in ("1", "true", "yes")

# Environment variables read by the example YAML files. Fixed values keep the
# snapshots independent of the machine running the tests.
FIXED_ENV = {
    "DEBUSSY_CONCERT__PROJECT": "snapshot-project",
    "PWD": "/snapshot/pwd",
    # Some compositions instantiate hooks while the DAG is parsed, which looks
    # up the connection. These dummy connections never reach a real system.
    "AIRFLOW_CONN_GOOGLE_CLOUD_DEBUSSY": "google-cloud-platform://",
    "AIRFLOW_CONN_MYSQL_DEV": "mysql://user:password@localhost:3306/sakila",
}

# Examples that do not build on the current code line. They are recorded as
# strict expected failures, so fixing one makes the test fail until its entry
# is removed here and a snapshot is created.
KNOWN_BROKEN = {
    "data_ingestion/mysql_sakila_ingestion/mysql_sakila_ingestion_yamless.py": (
        "RdbmsIngestionComposition reads dataproc_config['pyspark_script'], "
        "which only ConfigRdbmsDataIngestion.load_from_file fills in"
    ),
}
DAGS_FOLDER_PLACEHOLDER = "<DAGS_FOLDER>"


def _example_files():
    return sorted(
        path
        for path in EXAMPLES_DIR.rglob("*.py")
        if "scripts" not in path.relative_to(EXAMPLES_DIR).parts
    )


EXAMPLE_FILES = _example_files()


def _relative(example_file: Path) -> str:
    return example_file.relative_to(EXAMPLES_DIR).as_posix()


def _example_params():
    for path in EXAMPLE_FILES:
        reason = KNOWN_BROKEN.get(_relative(path))
        marks = [pytest.mark.xfail(reason=reason, strict=True)] if reason else []
        yield pytest.param(path, id=_relative(path), marks=marks)


@pytest.fixture
def example_environment(monkeypatch):
    """Point Airflow's dags_folder at the repo root, as the examples expect."""
    previous = conf.get("core", "dags_folder")
    conf.set("core", "dags_folder", str(REPO_ROOT))
    for key, value in FIXED_ENV.items():
        monkeypatch.setenv(key, value)
    inject.clear()
    yield
    inject.clear()
    conf.set("core", "dags_folder", previous)


def _build_dags(example_file: Path):
    namespace = runpy.run_path(str(example_file))
    dags = {
        value.dag_id: value for value in namespace.values() if isinstance(value, DAG)
    }
    assert dags, f"{example_file} did not define any DAG"
    return dags


def test_every_example_is_covered():
    assert EXAMPLE_FILES, "no example DAG files found"


def test_known_broken_entries_exist():
    known = {_relative(p) for p in EXAMPLE_FILES}
    assert set(KNOWN_BROKEN) <= known, sorted(set(KNOWN_BROKEN) - known)


@pytest.mark.parametrize("example_file", list(_example_params()))
def test_example_dag_matches_snapshot(example_environment, example_file):
    replacements = [(str(REPO_ROOT), DAGS_FOLDER_PLACEHOLDER)]
    for dag_id, dag in sorted(_build_dags(example_file).items()):
        actual = dump(dag_to_snapshot(dag, replacements))
        snapshot_file = SNAPSHOTS_DIR / f"{dag_id}.json"

        if UPDATE or not snapshot_file.exists():
            if not UPDATE:
                pytest.fail(
                    f"missing snapshot {snapshot_file.name}; "
                    "run with DEBUSSY_UPDATE_SNAPSHOTS=1 to create it"
                )
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
            snapshot_file.write_text(actual, encoding="utf-8")
            continue

        expected = snapshot_file.read_text(encoding="utf-8")
        assert actual == expected, (
            f"DAG {dag_id!r} built from {example_file.relative_to(REPO_ROOT)} "
            f"differs from {snapshot_file.relative_to(REPO_ROOT)}. If the change "
            "is intended, run with DEBUSSY_UPDATE_SNAPSHOTS=1 and review the diff."
        )


def test_no_orphan_snapshots(example_environment):
    """A snapshot whose example was removed or renamed must be deleted too."""
    expected = set()
    for example_file in EXAMPLE_FILES:
        if _relative(example_file) in KNOWN_BROKEN:
            continue
        inject.clear()
        expected.update(f"{dag_id}.json" for dag_id in _build_dags(example_file))
    existing = {p.name for p in SNAPSHOTS_DIR.glob("*.json")}
    assert existing <= expected, f"orphan snapshots: {sorted(existing - expected)}"
