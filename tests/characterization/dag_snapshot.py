"""Turn an Airflow DAG into a stable, JSON-serializable description.

The snapshot captures what Debussy generates, not how Airflow runs it:
DAG-level arguments, task groups, tasks (operator class, group, trigger rule,
upstream edges) and the raw value of every templated field. Values that are
not plain data are reduced to a stable name, so the snapshot never depends on
memory addresses, hash seeds or the machine it runs on.
"""
import dataclasses
import datetime as dt
import enum
import json
from pathlib import Path

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from debussy_concert.core.config.config_composition import ConfigComposition


def _qualified_name(obj) -> str:
    module = getattr(obj, "__module__", None) or ""
    name = getattr(obj, "__qualname__", None) or getattr(obj, "__name__", None)
    if name is None:
        name = type(obj).__qualname__
        module = type(obj).__module__
    return f"{module}.{name}" if module else name


def normalize(value, replacements):
    """Recursively convert ``value`` into JSON-friendly, deterministic data."""
    if isinstance(value, str):
        for old, new in replacements:
            value = value.replace(old, new)
        return value
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, enum.Enum):
        return normalize(value.value, replacements)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return f"timedelta(seconds={value.total_seconds()})"
    if isinstance(value, Path):
        return normalize(str(value), replacements)
    if isinstance(value, ConfigComposition):
        # The whole composition config is passed to some callables. It is the
        # input of the build, already covered by the YAML files, so only its
        # type and name are recorded.
        return f"<config {type(value).__qualname__} name={value.name}>"
    if isinstance(value, Message):
        return {
            f"<protobuf {type(value).DESCRIPTOR.full_name}>": normalize(
                MessageToDict(value), replacements
            )
        }
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = {f.name: getattr(value, f.name) for f in dataclasses.fields(value)}
        return {f"<{_qualified_name(type(value))}>": normalize(fields, replacements)}
    if isinstance(value, dict):
        return {
            str(normalize(k, replacements)): normalize(v, replacements)
            for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
        }
    if isinstance(value, (set, frozenset)):
        return sorted(
            (normalize(v, replacements) for v in value),
            key=lambda v: json.dumps(v, sort_keys=True),
        )
    if isinstance(value, (list, tuple)):
        return [normalize(v, replacements) for v in value]
    if callable(value):
        return f"<callable {_qualified_name(value)}>"
    return f"<{type(value).__module__}.{type(value).__qualname__}>"


def _schedule(dag):
    schedule = getattr(dag, "schedule_interval", None)
    return None if schedule is None else str(schedule)


def dag_to_snapshot(dag, replacements=()) -> dict:
    """Describe ``dag`` as plain data, sorted so that diffs stay readable."""
    replacements = tuple(replacements)

    def norm(value):
        return normalize(value, replacements)

    groups = {}
    for group_id, group in dag.task_group.get_task_group_dict().items():
        if group_id is None:  # the DAG's root group
            continue
        parent = group.parent_group.group_id if group.parent_group else None
        groups[group_id] = {"parent_group": parent}

    tasks = {}
    for task in sorted(dag.tasks, key=lambda t: t.task_id):
        task_group = task.task_group.group_id if task.task_group else None
        templated = {
            field: norm(getattr(task, field, None))
            for field in sorted(task.template_fields)
        }
        tasks[task.task_id] = {
            "operator": f"{type(task).__module__}.{type(task).__qualname__}",
            "task_group": task_group,
            "trigger_rule": norm(task.trigger_rule),
            "upstream": sorted(task.upstream_task_ids),
            "templated_fields": templated,
        }
        callable_ = getattr(task, "python_callable", None)
        if callable_ is not None:
            tasks[task.task_id]["python_callable"] = norm(callable_)

    return {
        "dag_id": dag.dag_id,
        "description": norm(dag.description),
        "schedule_interval": _schedule(dag),
        "start_date": norm(dag.start_date),
        "end_date": norm(dag.end_date),
        "catchup": dag.catchup,
        "max_active_runs": dag.max_active_runs,
        "tags": sorted(dag.tags or []),
        "default_args": norm(dag.default_args),
        "task_groups": dict(sorted(groups.items())),
        "tasks": tasks,
    }


def dump(snapshot: dict) -> str:
    return json.dumps(snapshot, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
