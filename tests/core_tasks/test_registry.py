import pytest

from debussy.core.tasks import ActionRegistry, TaskSpec, UnknownActionError


def test_register_and_resolve():
    registry = ActionRegistry("local")

    def run_query(**kwargs):
        return kwargs

    assert registry.register("bigquery.query", run_query) is run_query
    assert registry.resolve("bigquery.query") is run_query
    assert "bigquery.query" in registry
    assert len(registry) == 1
    assert registry.names() == {"bigquery.query"}


def test_register_as_decorator():
    registry = ActionRegistry("local")

    @registry.register("storage.copy")
    def copy(**kwargs):
        return kwargs

    assert registry.resolve("storage.copy") is copy


def test_duplicate_names_need_replace():
    registry = ActionRegistry("airflow")
    registry.register("a", 1)
    with pytest.raises(ValueError, match="already has an action named 'a'"):
        registry.register("a", 2)
    registry.register("a", 2, replace=True)
    assert registry.resolve("a") == 2


@pytest.mark.parametrize("name", ["", None])
def test_invalid_names(name):
    with pytest.raises(ValueError):
        ActionRegistry("local").register(name, 1)


def test_unknown_action():
    with pytest.raises(UnknownActionError) as error:
        ActionRegistry("local").resolve("dbt.run")
    assert error.value.backend == "local"
    assert error.value.action == "dbt.run"
    assert str(error.value) == "The local backend has no action named 'dbt.run'"
    assert isinstance(error.value, LookupError)


def test_resolve_spec():
    registry = ActionRegistry("local")
    registry.register("x.y", "implementation")

    def callable_action():
        pass

    assert registry.resolve_spec(TaskSpec(name="a", action="x.y")) == "implementation"
    assert registry.resolve_spec(TaskSpec(name="b", action=callable_action)) is (
        callable_action
    )
    with pytest.raises(UnknownActionError):
        registry.resolve_spec(TaskSpec(name="c", action="missing"))


def test_registries_are_independent():
    first, second = ActionRegistry("airflow"), ActionRegistry("local")
    first.register("a", 1)
    assert "a" not in second
