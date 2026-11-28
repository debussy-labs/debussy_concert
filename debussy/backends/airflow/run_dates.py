"""Run dates for Airflow 3 runs that have no logical date.

Airflow 3 can trigger a run without a logical date (the default for a manual
run from the UI or ``airflow dags trigger``). Such a run has no
``logical_date``, ``data_interval_*``, ``ds`` or ``ts*`` in its template
context, so every Debussy template, which reads the run's dates, fails with
``UndefinedError``.

A Debussy DAG treats such a run as if it had been triggered with its
``run_after`` time as logical date, which is what passing ``--logical-date``
with the trigger time does: the data interval comes from the DAG's timetable.
"""

import copy
from typing import Any, Mapping, MutableMapping, Optional, Tuple

import jinja2
import jinja2.runtime
from airflow.sdk import DAG


def _coerce(value):
    from airflow.sdk import timezone

    return timezone.coerce_datetime(value)


def _data_interval(dag, logical_date) -> Tuple[Any, Any]:
    try:
        from airflow.serialization.encoders import coerce_to_core_timetable

        timetable = coerce_to_core_timetable(dag.timetable)
        interval = timetable.infer_manual_data_interval(run_after=logical_date)
        return _coerce(interval.start), _coerce(interval.end)
    except Exception:  # no airflow-core on the worker, or an unusual timetable
        return logical_date, logical_date


def run_dates(dag, run_after) -> dict:
    """The date keys of the template context of a run at ``run_after``."""
    logical_date = _coerce(run_after)
    start, end = _data_interval(dag, logical_date)
    ds = logical_date.strftime("%Y-%m-%d")
    ts = logical_date.isoformat()
    return {
        "logical_date": logical_date,
        "ds": ds,
        "ds_nodash": ds.replace("-", ""),
        "ts": ts,
        "ts_nodash": logical_date.strftime("%Y%m%dT%H%M%S"),
        "ts_nodash_with_tz": ts.replace("-", "").replace(":", ""),
        "data_interval_start": start,
        "data_interval_end": end,
    }


def missing_run_dates(context: Mapping[str, Any]) -> dict:
    """The run dates ``context`` lacks because its run has no logical date."""
    if context.get("logical_date") is not None:
        return {}
    run_after: Optional[Any] = getattr(context.get("dag_run"), "run_after", None)
    if run_after is None:
        return {}
    return {
        key: value
        for key, value in run_dates(context.get("dag"), run_after).items()
        if context.get(key) is None
    }


def fill_run_dates(context: MutableMapping[str, Any]) -> None:
    """Add the run dates to ``context`` when the run has no logical date."""
    context.update(missing_run_dates(context))


class RunDatesContext(jinja2.runtime.Context):
    """Jinja context that adds the run dates when the run has none.

    Airflow renders through ``environment.context_class`` directly, without
    ``Template.render``, so the dates are added here."""

    def __init__(self, environment, parent, name, blocks, globals=None):
        if dates := missing_run_dates(parent):
            parent = copy.copy(parent)
            parent.update(dates)
        super().__init__(environment, parent, name, blocks, globals)


class DebussyDAG(DAG):
    """An Airflow DAG whose templates also render in runs without a logical
    date. Everything else is Airflow's ``DAG``."""

    def get_template_env(self, *, force_sandboxed: bool = False) -> jinja2.Environment:
        env = super().get_template_env(force_sandboxed=force_sandboxed)
        env.context_class = RunDatesContext
        return env
