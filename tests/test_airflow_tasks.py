"""The Airflow backend builds operators from engine-neutral task specs."""

import datetime as dt

import pytest

pytest.importorskip("airflow")

import pendulum  # noqa: E402
from airflow.providers.standard.operators.empty import EmptyOperator  # noqa: E402
from airflow.providers.standard.operators.python import PythonOperator  # noqa: E402
from airflow.sdk import DAG, TaskGroup  # noqa: E402

from debussy.backends.airflow import tasks as airflow_tasks  # noqa: E402
from debussy.backends.airflow.service import AirflowService  # noqa: E402
from debussy.backends.airflow.tasks import build_task  # noqa: E402
from debussy.core.motif.motif_base import MotifBase  # noqa: E402
from debussy.core.tasks import (  # noqa: E402
    ActionRegistry,
    Capability,
    TaskSpec,
    UnknownActionError,
    UnsupportedCapabilityError,
)

LOGICAL_DATE = pendulum.datetime(2026, 3, 4, 5, 6, 7, tz="UTC")


def greet(name, day):
    return f"hello {name} on {day}"


@pytest.fixture
def dag():
    return DAG(dag_id="tasks_test", schedule=None, start_date=dt.datetime(2026, 1, 1))


def _render(operator):
    context = {"logical_date": LOGICAL_DATE, "ds": LOGICAL_DATE.strftime("%Y-%m-%d")}
    operator.render_template_fields(context)
    return context


def test_callable_becomes_a_python_operator(dag):
    spec = TaskSpec(name="greet", action=greet, params={"name": "a", "day": "d"})
    operator = build_task(spec, dag=dag)
    assert isinstance(operator, PythonOperator)
    assert operator.task_id == "greet"
    assert operator.python_callable is greet
    assert operator.dag is dag


def test_only_templated_params_are_rendered(dag):
    spec = TaskSpec(
        name="greet",
        action=greet,
        params={"name": "{{ ds }}", "day": "{{ ds }}"},
        templated={"day"},
    )
    operator = build_task(spec, dag=dag)
    context = _render(operator)
    assert operator.op_kwargs == {"name": "{{ ds }}", "day": "2026-03-04"}
    assert operator.execute(context) == "hello {{ ds }} on 2026-03-04"


def test_objects_are_passed_as_they_are(dag):
    config = object()
    spec = TaskSpec(name="t", action=greet, params={"name": config, "day": 1})
    operator = build_task(spec, dag=dag)
    assert operator.op_kwargs == {"name": config, "day": 1}


def test_retries(dag):
    spec = TaskSpec(name="t", action=greet, retries=3)
    assert build_task(spec, dag=dag).retries == 3


def test_task_group(dag):
    group = TaskGroup(group_id="Motif", dag=dag)
    operator = build_task(TaskSpec(name="t", action=greet), dag=dag, task_group=group)
    assert operator.task_id == "Motif.t"


def test_named_action_uses_the_registered_factory(dag):
    registry = ActionRegistry("airflow")
    calls = []

    @registry.register("empty")
    def empty(spec, *, dag, task_group, **kwargs):
        calls.append(kwargs)
        return EmptyOperator(
            task_id=spec.name, dag=dag, task_group=task_group, **kwargs
        )

    spec = TaskSpec(name="nothing", action="empty", retries=1)
    operator = build_task(spec, dag=dag, registry=registry)
    assert isinstance(operator, EmptyOperator)
    assert operator.task_id == "nothing"
    assert calls == [{"retries": 1}]


def test_unknown_named_action(dag):
    with pytest.raises(UnknownActionError, match="airflow backend"):
        build_task(TaskSpec(name="t", action="no.such.action"), dag=dag)


def test_the_default_registry_is_the_backend_registry():
    assert airflow_tasks.actions.backend == "airflow"


def test_capabilities_are_checked(dag, monkeypatch):
    assert airflow_tasks.AIRFLOW_CAPABILITIES == frozenset(Capability)
    monkeypatch.setattr(airflow_tasks, "AIRFLOW_CAPABILITIES", frozenset())
    with pytest.raises(UnsupportedCapabilityError, match="Task 't'.*retries"):
        build_task(TaskSpec(name="t", action=greet, retries=1), dag=dag)


class GreetMotif(MotifBase):
    def tasks(self):
        return [
            TaskSpec(name="first", action=greet, params={"name": "a", "day": "1"}),
            TaskSpec(name="second", action=greet, params={"name": "b", "day": "2"}),
            TaskSpec(name="third", action=greet, params={"name": "c", "day": "3"}),
        ]


class SingleTaskMotif(MotifBase):
    group_tasks = False

    def tasks(self):
        return [TaskSpec(name=self.name, action=greet)]


def _motif(cls, **kwargs):
    return cls(workflow_service=AirflowService(), config=None, **kwargs)


def test_motif_tasks_are_grouped_and_chained(dag):
    phrase = AirflowService.phrase_group("Phrase", dag, None)
    group = _motif(GreetMotif, name="Greet").build(dag, phrase)
    assert isinstance(group, TaskGroup)
    assert group.group_id == "Phrase.Greet"
    first, second, third = (
        dag.get_task(f"Phrase.Greet.{n}") for n in ("first", "second", "third")
    )
    assert first.upstream_task_ids == set()
    assert second.upstream_task_ids == {first.task_id}
    assert third.upstream_task_ids == {second.task_id}


def test_single_task_motif_without_group(dag):
    phrase = AirflowService.phrase_group("Phrase", dag, None)
    operator = _motif(SingleTaskMotif, name="End").build(dag, phrase)
    assert isinstance(operator, PythonOperator)
    assert operator.task_id == "Phrase.End"


def test_several_tasks_need_a_group(dag):
    specs = [TaskSpec(name="a", action=greet), TaskSpec(name="b", action=greet)]
    with pytest.raises(ValueError, match="has 2 tasks"):
        AirflowService.build_tasks("M", specs, dag, None, grouped=False)


def test_a_motif_needs_tasks(dag):
    with pytest.raises(ValueError, match="has no tasks"):
        AirflowService.build_tasks("M", [], dag, None)


def test_duplicate_task_names(dag):
    specs = [TaskSpec(name="a", action=greet), TaskSpec(name="a", action=greet)]
    with pytest.raises(ValueError, match="Duplicate task name"):
        AirflowService.build_tasks("M", specs, dag, None)
