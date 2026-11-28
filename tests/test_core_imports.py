"""The core must import without Airflow, a cloud SDK or dbt.

Airflow-specific code lives in ``debussy.backends.airflow``. Motifs describe
their work with the engine-neutral task contract (``debussy.core.tasks``), so
they import without Airflow; the pipeline compositions still build Airflow
objects directly. Every other module has to import with Airflow blocked, even
when Airflow is installed.
"""

import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "debussy"

# Directories whose modules need Airflow.
AIRFLOW_ONLY_DIRS = {"backends", "composition"}
# ``composition`` is Airflow-only only below ``pipeline``: the core version of
# this package must stay importable.
CORE_DIRS = {PACKAGE_ROOT / "core"}


def _is_airflow_only(path: Path) -> bool:
    if any(core in path.parents for core in CORE_DIRS):
        return False
    return bool(AIRFLOW_ONLY_DIRS & set(path.relative_to(PACKAGE_ROOT).parts[:-1]))


def _module_names():
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if path.name == "__init__.py" or _is_airflow_only(path):
            continue
        relative = path.relative_to(PACKAGE_ROOT.parent).with_suffix("")
        yield ".".join(relative.parts)


def test_core_modules_import_without_airflow():
    modules = list(_module_names())
    assert "debussy.core.composition.composition_base" in modules
    assert "debussy.pipeline.reverse_etl.config.reverse_etl" in modules
    assert "debussy.core.tasks.context" in modules
    assert "debussy.pipeline.reverse_etl.motif.bigquery_query_job" in modules
    assert "debussy.pipeline.data_ingestion.motif.export_table" in modules

    script = (
        "import importlib, sys\n"
        "sys.modules['airflow'] = None  # `import airflow...` raises ImportError\n"
        f"for name in {modules!r}:\n"
        "    importlib.import_module(name)\n"
        "leaked = sorted(m for m in sys.modules if m.split('.')[0] == 'airflow' "
        "and sys.modules[m] is not None)\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
