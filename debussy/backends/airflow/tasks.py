"""Turns engine-neutral ``TaskSpec`` objects into Airflow operators."""

import dataclasses
from typing import Any, Callable, Mapping

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator, TriggerRule, literal

from debussy.core.tasks import (
    ActionRegistry,
    Capability,
    TaskSpec,
    check_capabilities,
)
from debussy.core.tasks.spec import TASK_RESULT_PATTERN

#: Airflow supports every capability of the task contract.
AIRFLOW_CAPABILITIES = frozenset(Capability)

#: An operator factory for a named action. It is called with the spec, the DAG
#: and task group to build in, and extra operator arguments (for example
#: ``retries``), and returns the operator. It names the operator ``spec.name``
#: and passes the templated params to templated operator fields.
OperatorFactory = Callable[..., BaseOperator]

#: Named actions of the Airflow backend.
actions: ActionRegistry = ActionRegistry("airflow")

# Values Airflow renders when they are in a templated field. Other objects are
# rendered only if they declare template fields themselves, which params do not.
_RENDERED_TYPES = (str, list, tuple, dict, set)


def build_task(
    spec: TaskSpec,
    *,
    dag,
    task_group=None,
    registry: ActionRegistry = actions,
) -> BaseOperator:
    """Build the operator for one spec.

    A callable action becomes a ``PythonOperator`` that calls it with the
    params as keyword arguments. Airflow renders only the templated params:
    the others are passed through ``literal()``. Its return value goes to
    XCom, so later templates can read it with ``ti.xcom_pull``. A named
    action is built by the operator factory registered for it.
    """
    check_capabilities(
        spec.required_capabilities,
        AIRFLOW_CAPABILITIES,
        subject=f"Task {spec.name!r}",
    )
    operator_kwargs = {}
    if spec.retries is not None:
        operator_kwargs["retries"] = spec.retries
    if spec.always_run:
        operator_kwargs["trigger_rule"] = TriggerRule.ALL_DONE

    if spec.is_named_action:
        factory: OperatorFactory = registry.resolve(spec.action)
        return factory(spec, dag=dag, task_group=task_group, **operator_kwargs)

    return PythonOperator(
        task_id=spec.name,
        python_callable=spec.action,
        op_kwargs={
            key: _param_value(spec, key, value) for key, value in spec.params.items()
        },
        dag=dag,
        task_group=task_group,
        **operator_kwargs,
    )


def resolve_task_results(
    spec: TaskSpec, task_ids: Mapping[str, str], dag_id: str
) -> TaskSpec:
    """``spec`` with each ``result_of(name)`` in its templated params turned
    into an XCom pull of that task.

    ``task_ids`` maps the names of the tasks already built in the motif to
    their Airflow task ids. A reference to any other task raises
    ``ValueError``.
    """
    if not spec.templated:
        return spec

    def replace(match):
        name = match.group(1)
        if name not in task_ids:
            raise ValueError(
                f"Task {spec.name!r} uses the result of {name!r}, which is not"
                " an earlier task of its motif"
            )
        return f"ti.xcom_pull(dag_id='{dag_id}', task_ids='{task_ids[name]}')"

    def resolve(value):
        if isinstance(value, str):
            return TASK_RESULT_PATTERN.sub(replace, value)
        if isinstance(value, (list, tuple)):
            return type(value)(resolve(item) for item in value)
        if isinstance(value, Mapping):
            return {key: resolve(item) for key, item in value.items()}
        return value

    params = {
        key: resolve(value) if key in spec.templated else value
        for key, value in spec.params.items()
    }
    return dataclasses.replace(spec, params=params)


def _param_value(spec: TaskSpec, key: str, value: Any) -> Any:
    if key in spec.templated or not isinstance(value, _RENDERED_TYPES):
        return value
    return literal(value)


# Registers the backend's named actions. Imported last because it imports
# ``actions`` from this module.
from debussy.backends.airflow import operator_actions  # noqa: E402, F401
