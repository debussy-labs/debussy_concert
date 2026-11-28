"""Debussy DAGs render their templates in runs without a logical date."""

import datetime as dt
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow")

import pendulum  # noqa: E402
from airflow.sdk.definitions.context import render_template  # noqa: E402

from debussy.backends.airflow.run_dates import (  # noqa: E402
    DebussyDAG,
    fill_run_dates,
)
from debussy.backends.airflow.service import AirflowService  # noqa: E402

RUN_AFTER = pendulum.datetime(2026, 3, 4, 5, 6, 7, tz="UTC")
TEMPLATE = (
    "{{ logical_date.strftime('%Y-%m-%d') }}|{{ ds }}|{{ ts_nodash }}"
    "|{{ data_interval_end.strftime('%Y-%m-%d 00:00:00') }}"
)


def _dag(**kwargs):
    return AirflowService.workflow_dag(
        "run_dates", schedule="0 3 * * *", start_date=dt.datetime(2026, 1, 1), **kwargs
    )


def _dateless_context(dag):
    # What Airflow 3 passes for a run triggered without a logical date.
    return {"dag": dag, "dag_run": SimpleNamespace(run_after=RUN_AFTER)}


def test_workflow_dags_are_debussy_dags():
    assert isinstance(_dag(), DebussyDAG)


@pytest.mark.parametrize("native", [False, True])
def test_templates_render_without_a_logical_date(native):
    dag = _dag(render_template_as_native_obj=native)
    template = dag.get_template_env().from_string(TEMPLATE)
    expected = "2026-03-04|2026-03-04|20260304T050607|2026-03-04 00:00:00"
    # Airflow renders through the environment's context class ...
    assert render_template(template, _dateless_context(dag), native=native) == expected
    # ... and Template.render does too.
    assert template.render(**_dateless_context(dag)) == expected


def test_the_logical_date_of_the_run_wins():
    dag = _dag()
    logical_date = pendulum.datetime(2022, 1, 1, tz="UTC")
    context = {
        **_dateless_context(dag),
        "logical_date": logical_date,
        "ds": "2022-01-01",
        "ts_nodash": "20220101T000000",
        "data_interval_end": logical_date,
    }
    rendered = dag.get_template_env().from_string(TEMPLATE).render(**context)
    assert rendered == "2022-01-01|2022-01-01|20220101T000000|2022-01-01 00:00:00"


def test_fill_run_dates_uses_the_timetable():
    context = _dateless_context(_dag())
    fill_run_dates(context)
    assert context["logical_date"] == RUN_AFTER
    assert context["ds_nodash"] == "20260304"
    # A cron schedule is a CronTriggerTimetable: the interval is the run time.
    assert context["data_interval_start"] == context["data_interval_end"] == RUN_AFTER


def test_fill_run_dates_needs_run_after():
    context = {"dag": _dag(), "dag_run": None}
    fill_run_dates(context)
    assert "logical_date" not in context
