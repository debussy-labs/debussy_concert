"""Run-time values and template rendering, without Airflow."""

import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from types import MappingProxyType, SimpleNamespace
from typing import Any, Mapping, Optional

import jinja2

# Namespace for the deterministic task instance ids exposed as ``ti.id``.
_TASK_INSTANCE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "debussy:task-instance")


def _aware(value: Optional[datetime]) -> Optional[datetime]:
    """``value`` with a timezone. A naive datetime is taken as UTC, like
    Airflow does.

    Plain datetimes print like the pendulum ones Airflow 3 passes to templates
    (``2026-01-02 03:04:05+00:00``), so ``{{ logical_date }}`` renders the same.
    """
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"Expected a datetime, got {type(value).__name__}")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def ds_add(ds: str, days: int) -> str:
    """Add ``days`` to a ``YYYY-MM-DD`` date (Airflow's ``macros.ds_add``)."""
    if not days:
        return str(ds)
    return (datetime.strptime(str(ds), "%Y-%m-%d") + timedelta(days=days)).strftime(
        "%Y-%m-%d"
    )


def ds_format(ds: str, input_format: str, output_format: str) -> str:
    """Reformat a date string (Airflow's ``macros.ds_format``)."""
    return datetime.strptime(str(ds), input_format).strftime(output_format)


#: The ``macros`` namespace: the subset of Airflow's macros Debussy supports.
MACROS = SimpleNamespace(
    ds_add=ds_add,
    ds_format=ds_format,
    datetime=datetime,
    timedelta=timedelta,
    uuid=uuid,
)

# One environment for every render. StrictUndefined is Airflow's default too: a
# name the context does not define fails instead of rendering as empty.
_ENVIRONMENT = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=False)


class _TaskInstance:
    """The part of Airflow's ``ti`` that Debussy templates use."""

    def __init__(self, context: "RunContext", task_name: Optional[str]) -> None:
        self._context = context
        self.task_id = task_name
        self.run_id = context.run_id
        self.dag_id = context.dag_id
        self.id = uuid.uuid5(
            _TASK_INSTANCE_NAMESPACE,
            f"{context.dag_id}/{context.run_id}/{task_name or ''}",
        )

    def xcom_pull(self, task_ids=None, dag_id=None, key="return_value", **_):
        """The result of earlier tasks, from ``RunContext.results``.

        Like Airflow, a single task id returns that task's value (``None`` if
        it has none) and a list of ids returns a list. Only the default key is
        supported, and ``dag_id`` must be this run's pipeline if given.
        """
        if key != "return_value":
            raise NotImplementedError(
                f"xcom_pull with key={key!r}: only task return values are supported"
            )
        if dag_id is not None and self.dag_id and dag_id != self.dag_id:
            raise NotImplementedError(
                f"xcom_pull from another pipeline ({dag_id!r}) is not supported"
            )
        if task_ids is None:
            raise ValueError("xcom_pull needs task_ids")
        if isinstance(task_ids, str):
            return self._context.results.get(task_ids)
        return [self._context.results.get(task_id) for task_id in task_ids]


@dataclass(frozen=True)
class RunContext:
    """The values a task can use at run time, and the templates they fill.

    The names exposed to templates are the ones Debussy pipelines use with
    Airflow (``logical_date``, ``data_interval_end``, ``dag_run.start_date``,
    ``ts_nodash``, ``macros.ds_format``, ``ti.id``, ``ti.xcom_pull`` and a few
    more, see ``template_variables``), so a pipeline's YAML renders the same
    way on every backend. ``task_result(name)``, written by
    ``debussy.core.tasks.result_of``, reads ``results``: the executor gives
    each task a context whose results are keyed by the task names of its
    motif.
    """

    #: The date the run is for. Naive datetimes are taken as UTC.
    logical_date: datetime
    #: Start of the data interval. Defaults to ``logical_date``.
    data_interval_start: Optional[datetime] = None
    #: End of the data interval. Defaults to ``logical_date``.
    data_interval_end: Optional[datetime] = None
    #: When the run started. Defaults to now.
    run_start: Optional[datetime] = None
    #: Identifies the run.
    run_id: str = ""
    #: The pipeline's id (the DAG id on Airflow).
    dag_id: str = ""
    #: Run parameters, available as ``params`` in templates.
    params: Mapping[str, Any] = field(default_factory=dict)
    #: Return values of the tasks that already ran, by task id.
    results: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        logical_date = _aware(self.logical_date)
        if logical_date is None:
            raise ValueError("logical_date is required")
        set_ = object.__setattr__
        set_(self, "logical_date", logical_date)
        set_(
            self,
            "data_interval_start",
            _aware(self.data_interval_start or logical_date),
        )
        set_(
            self,
            "data_interval_end",
            _aware(self.data_interval_end or logical_date),
        )
        set_(
            self,
            "run_start",
            _aware(self.run_start or datetime.now(timezone.utc)),
        )
        set_(self, "params", MappingProxyType(dict(self.params)))
        set_(self, "results", MappingProxyType(dict(self.results)))

    def with_result(self, task_id: str, value: Any) -> "RunContext":
        """A copy of this context that also holds ``task_id``'s result."""
        return replace(self, results={**self.results, task_id: value})

    def template_variables(self, task_name: Optional[str] = None) -> dict:
        """The names templates can use. ``task_name`` sets ``ti.task_id`` and
        ``ti.id``."""
        logical_date = self.logical_date
        ds = logical_date.strftime("%Y-%m-%d")
        ts = logical_date.isoformat()
        return {
            "logical_date": logical_date,
            "data_interval_start": self.data_interval_start,
            "data_interval_end": self.data_interval_end,
            "ds": ds,
            "ds_nodash": ds.replace("-", ""),
            "ts": ts,
            "ts_nodash": logical_date.strftime("%Y%m%dT%H%M%S"),
            "ts_nodash_with_tz": ts.replace("-", "").replace(":", ""),
            "run_id": self.run_id,
            "params": dict(self.params),
            "dag": SimpleNamespace(dag_id=self.dag_id),
            "dag_run": SimpleNamespace(
                dag_id=self.dag_id,
                run_id=self.run_id,
                logical_date=logical_date,
                data_interval_start=self.data_interval_start,
                data_interval_end=self.data_interval_end,
                start_date=self.run_start,
            ),
            "ti": _TaskInstance(self, task_name),
            "task_result": self.results.get,
            "macros": MACROS,
        }

    def render(self, template: str, *, task_name: Optional[str] = None) -> str:
        """Render ``template`` with Jinja2 and ``template_variables``."""
        return _ENVIRONMENT.from_string(template).render(
            self.template_variables(task_name)
        )
