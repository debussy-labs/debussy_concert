from datetime import datetime, timezone

import pytest

from debussy.core.tasks import (
    Capability,
    RunContext,
    TaskSpec,
    required_capabilities,
    validate_tasks,
)


def noop(**kwargs):
    return kwargs


def test_spec_defaults():
    spec = TaskSpec(name="load", action=noop)
    assert spec.params == {}
    assert spec.templated == frozenset()
    assert spec.requires == frozenset()
    assert spec.retries is None
    assert not spec.is_named_action
    assert spec.required_capabilities == frozenset()


def test_named_action():
    spec = TaskSpec(name="query", action="bigquery.query")
    assert spec.is_named_action


@pytest.mark.parametrize("name", ["", ".hidden", "-x", "has space", "a/b", None, 3])
def test_invalid_names(name):
    with pytest.raises(ValueError, match="Invalid task name"):
        TaskSpec(name=name, action=noop)


@pytest.mark.parametrize("name", ["a", "begin_dag", "log-input", "Motif.task_1"])
def test_valid_names(name):
    assert TaskSpec(name=name, action=noop).name == name


def test_empty_action_name():
    with pytest.raises(ValueError, match="action name is empty"):
        TaskSpec(name="t", action="")


def test_action_must_be_callable_or_name():
    with pytest.raises(TypeError, match="callable or an action name"):
        TaskSpec(name="t", action=42)


@pytest.mark.parametrize("retries", [-1, 1.5, True, "2"])
def test_invalid_retries(retries):
    with pytest.raises(ValueError, match="retries"):
        TaskSpec(name="t", action=noop, retries=retries)


def test_templated_must_name_params():
    with pytest.raises(ValueError, match="templated names are not params: sql"):
        TaskSpec(name="t", action=noop, params={"query": "x"}, templated={"sql"})


def test_spec_is_immutable_and_copies_inputs():
    params = {"sql": "select 1"}
    spec = TaskSpec(name="t", action=noop, params=params, templated=["sql"])
    params["sql"] = "changed"
    assert spec.params["sql"] == "select 1"
    with pytest.raises(TypeError):
        spec.params["sql"] = "x"
    with pytest.raises(AttributeError):
        spec.name = "other"
    assert spec.templated == frozenset({"sql"})


def test_single_strings_are_not_split():
    spec = TaskSpec(
        name="t",
        action=noop,
        params={"sql": "x"},
        templated="sql",
        requires="xcom",
    )
    assert spec.templated == frozenset({"sql"})
    assert spec.requires == frozenset({Capability.XCOM})


def test_unknown_capability():
    with pytest.raises(ValueError):
        TaskSpec(name="t", action=noop, requires={"teleport"})


def test_required_capabilities_include_implied_ones():
    spec = TaskSpec(
        name="t",
        action=noop,
        params={"sql": "x"},
        templated={"sql"},
        retries=2,
        requires={Capability.NATIVE_OPERATOR},
    )
    assert spec.required_capabilities == {
        Capability.NATIVE_OPERATOR,
        Capability.TEMPLATING,
        Capability.RETRIES,
    }
    # Zero retries needs no retry support.
    assert TaskSpec(name="t", action=noop, retries=0).required_capabilities == set()


def test_render_params_only_renders_templated_values():
    context = RunContext(logical_date=datetime(2026, 1, 2, tzinfo=timezone.utc))
    spec = TaskSpec(
        name="t",
        action=noop,
        params={
            "sql": "select '{{ ds }}'",
            "labels": ["{{ ds_nodash }}", 3],
            "config": {"date": ("{{ ds }}",), "n": 1},
            "raw": "{{ ds }}",
        },
        templated={"sql", "labels", "config"},
    )
    assert spec.render_params(context) == {
        "sql": "select '2026-01-02'",
        "labels": ["20260102", 3],
        "config": {"date": ("2026-01-02",), "n": 1},
        "raw": "{{ ds }}",
    }


def test_render_params_passes_the_task_name():
    context = RunContext(
        logical_date=datetime(2026, 1, 2, tzinfo=timezone.utc), run_id="r"
    )
    spec = TaskSpec(
        name="t1", action=noop, params={"x": "{{ ti.task_id }}"}, templated={"x"}
    )
    assert spec.render_params(context) == {"x": "t1"}


def test_validate_tasks():
    first = TaskSpec(name="a", action=noop)
    second = TaskSpec(name="b", action="x.y")
    assert validate_tasks([first, second]) == (first, second)
    assert validate_tasks(iter([first])) == (first,)
    assert validate_tasks([]) == ()


def test_validate_tasks_rejects_duplicates():
    with pytest.raises(ValueError, match="Duplicate task name 'a'"):
        validate_tasks(
            [TaskSpec(name="a", action=noop), TaskSpec(name="a", action=noop)]
        )


def test_validate_tasks_rejects_other_objects():
    with pytest.raises(TypeError, match="Expected a TaskSpec"):
        validate_tasks([object()])


def test_required_capabilities_of_several_specs():
    specs = [
        TaskSpec(name="a", action=noop, retries=1),
        TaskSpec(name="b", action=noop, requires={Capability.XCOM}),
    ]
    assert required_capabilities(specs) == {Capability.RETRIES, Capability.XCOM}
    assert required_capabilities([]) == frozenset()
