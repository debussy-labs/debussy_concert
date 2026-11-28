import uuid
from datetime import datetime, timedelta, timezone

import jinja2
import pytest

from debussy.core.tasks import RunContext

LOGICAL_DATE = datetime(2026, 3, 4, 5, 6, 7, tzinfo=timezone.utc)
RUN_START = datetime(2026, 3, 5, 1, 2, 3, tzinfo=timezone.utc)


@pytest.fixture
def context():
    return RunContext(
        logical_date=LOGICAL_DATE,
        data_interval_start=LOGICAL_DATE - timedelta(days=1),
        data_interval_end=LOGICAL_DATE + timedelta(days=1),
        run_start=RUN_START,
        run_id="manual__1",
        dag_id="pipeline",
        params={"table": "film"},
    )


def test_defaults():
    context = RunContext(logical_date=LOGICAL_DATE)
    assert context.data_interval_start == LOGICAL_DATE
    assert context.data_interval_end == LOGICAL_DATE
    assert context.run_start.tzinfo is not None
    assert context.params == {}
    assert context.results == {}


def test_naive_dates_are_utc():
    context = RunContext(logical_date=datetime(2026, 1, 1))
    assert context.logical_date == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_logical_date_is_required():
    with pytest.raises(ValueError):
        RunContext(logical_date=None)
    with pytest.raises(TypeError):
        RunContext(logical_date="2026-01-01")


def test_dates_print_like_airflow(context):
    # Airflow 3 passes pendulum 3 datetimes, which print with a space.
    assert str(context.logical_date) == "2026-03-04 05:06:07+00:00"


def test_context_is_immutable(context):
    with pytest.raises(AttributeError):
        context.run_id = "x"
    with pytest.raises(TypeError):
        context.params["table"] = "x"


@pytest.mark.parametrize(
    "template, expected",
    [
        ("{{ logical_date }}", "2026-03-04 05:06:07+00:00"),
        ("{{ logical_date.strftime('%Y-%m-%d 00:00:00') }}", "2026-03-04 00:00:00"),
        ("{{ data_interval_start.strftime('%Y%m%d') }}", "20260303"),
        (
            "{{ data_interval_end.strftime('%Y-%m-%d 00:00:00') }}",
            "2026-03-05 00:00:00",
        ),
        (
            "{{ dag_run.start_date.strftime('%Y-%m-%d %H:%M:%S%z') }}",
            "2026-03-05 01:02:03+0000",
        ),
        ("{{ dag_run.start_date }}", "2026-03-05 01:02:03+00:00"),
        ("{{ ds }}|{{ ds_nodash }}", "2026-03-04|20260304"),
        ("{{ ts }}", "2026-03-04T05:06:07+00:00"),
        ("{{ ts_nodash }}", "20260304T050607"),
        ("{{ ts_nodash_with_tz }}", "20260304T050607+0000"),
        (
            "{{ macros.ds_format(ts, '%Y-%m-%dT%H:%M:%S+00:00', '%Y%m%d%H') }}",
            "2026030405",
        ),
        ("{{ macros.ds_add(ds, -5) }}", "2026-02-27"),
        ("{{ macros.ds_add(ds, 0) }}", "2026-03-04"),
        ("{{ params.table }}", "film"),
        ("{{ run_id }}|{{ dag_run.run_id }}", "manual__1|manual__1"),
        ("{{ dag.dag_id }}|{{ dag_run.dag_id }}", "pipeline|pipeline"),
        ("plain text", "plain text"),
    ],
)
def test_render(context, template, expected):
    assert context.render(template) == expected


def test_undefined_names_fail(context):
    with pytest.raises(jinja2.UndefinedError):
        context.render("{{ execution_date }}")


def test_task_instance_id_is_stable_per_task(context):
    first = context.render("{{ ti.id.hex[:8] }}", task_name="a")
    assert len(first) == 8
    assert first == context.render("{{ ti.id.hex[:8] }}", task_name="a")
    assert first != context.render("{{ ti.id.hex[:8] }}", task_name="b")
    assert isinstance(context.template_variables("a")["ti"].id, uuid.UUID)
    assert context.render("{{ ti.task_id }}", task_name="a") == "a"


def test_xcom_pull_reads_results(context):
    context = context.with_result("group.cluster_name_id", "cluster-1")
    assert (
        context.render(
            "{{ ti.xcom_pull(dag_id='pipeline', task_ids='group.cluster_name_id') }}"
        )
        == "cluster-1"
    )
    assert context.render("{{ ti.xcom_pull(task_ids='missing') }}") == "None"
    ti = context.template_variables()["ti"]
    assert ti.xcom_pull(task_ids=["group.cluster_name_id", "missing"]) == [
        "cluster-1",
        None,
    ]


def test_with_result_returns_a_copy(context):
    updated = context.with_result("a", 1).with_result("b", 2)
    assert updated.results == {"a": 1, "b": 2}
    assert context.results == {}
    assert updated.logical_date == context.logical_date


def test_xcom_pull_limits(context):
    ti = context.template_variables()["ti"]
    with pytest.raises(NotImplementedError, match="key='other'"):
        ti.xcom_pull(task_ids="a", key="other")
    with pytest.raises(NotImplementedError, match="another pipeline"):
        ti.xcom_pull(task_ids="a", dag_id="other_pipeline")
    with pytest.raises(ValueError):
        ti.xcom_pull()
