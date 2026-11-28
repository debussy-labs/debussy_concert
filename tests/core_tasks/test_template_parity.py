"""Every template Debussy generates renders locally as it does on Airflow.

The templates come from the Phase 0 snapshots, which hold the raw value of
every templated field of every example DAG. The Airflow side renders them with
Airflow's own Jinja environment and macros and pendulum datetimes, as Airflow
passes them, so this catches a template name or a date format that
``RunContext`` gets wrong.
"""

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from debussy.core.tasks import RunContext
from debussy.core.tasks.context import _ENVIRONMENT

SNAPSHOTS = Path(__file__).resolve().parents[1] / "characterization" / "snapshots"


def _templates(value):
    if isinstance(value, str):
        if "{{" in value or "{%" in value:
            yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _templates(item)
    elif isinstance(value, list):
        for item in value:
            yield from _templates(item)


def snapshot_templates():
    """(DAG id, template) for every distinct template in the snapshots."""
    found = set()
    for path in sorted(SNAPSHOTS.glob("*.json")):
        for template in _templates(json.loads(path.read_text())):
            found.add((path.stem, template))
    return sorted(found)


def test_snapshots_have_templates():
    # Guards the tests below against silently checking nothing.
    assert len(snapshot_templates()) > 50


@pytest.mark.parametrize("dag_id, template", snapshot_templates())
def test_snapshot_template_renders_locally(dag_id, template):
    context = RunContext(
        logical_date=datetime(2026, 1, 2, 3),
        dag_id=dag_id,
        run_id="manual__test",
    )
    context.render(template, task_name="task")


def test_templates_render_like_airflow():
    pendulum = pytest.importorskip("pendulum")
    pytest.importorskip("airflow")
    from airflow.sdk import DAG
    from airflow.sdk.execution_time import macros

    logical_date = pendulum.datetime(2026, 3, 4, 5, 6, 7, tz="UTC")
    start = logical_date + timedelta(hours=20)
    ti_id = uuid.uuid4()
    environment = DAG(dag_id="parity").get_template_env()
    airflow_variables = {
        "logical_date": logical_date,
        "data_interval_start": logical_date - timedelta(days=1),
        "data_interval_end": logical_date,
        "ds": logical_date.strftime("%Y-%m-%d"),
        "ts": logical_date.isoformat(),
        "ts_nodash": logical_date.strftime("%Y%m%dT%H%M%S"),
        "dag_run": SimpleNamespace(start_date=start),
        "macros": macros,
        # No task ran, so every xcom_pull returns None on both sides.
        "ti": SimpleNamespace(id=ti_id, xcom_pull=lambda **_: None),
    }

    for dag_id, template in snapshot_templates():
        local = RunContext(
            logical_date=logical_date,
            data_interval_start=logical_date - timedelta(days=1),
            data_interval_end=logical_date,
            run_start=start,
            dag_id=dag_id,
        )
        local_variables = local.template_variables("task")
        # ti.id is random on Airflow: use the same one on both sides.
        local_variables["ti"].id = ti_id
        expected = _outcome(environment, template, airflow_variables)
        actual = _outcome(_ENVIRONMENT, template, local_variables)
        assert actual == expected, template


def _outcome(environment, template, variables):
    """The rendered text, or the type of the error rendering raised."""
    try:
        return environment.from_string(template).render(variables)
    except Exception as error:
        return type(error)
