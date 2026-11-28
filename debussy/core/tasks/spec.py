"""The engine-neutral description of one unit of work."""

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, Optional, Union

from debussy.core.tasks.capabilities import Capability

if TYPE_CHECKING:
    from debussy.core.tasks.context import RunContext

#: A Python callable, or the name of an action each backend implements (for
#: example ``"bigquery.query"``), resolved through an ``ActionRegistry``.
Action = Union[Callable[..., Any], str]

# Characters a task name may use: the ones every backend accepts as a task id.
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


@dataclass(frozen=True)
class TaskSpec:
    """What one task runs, without naming the engine that runs it.

    A motif describes its work as a sequence of specs, which run in order. The
    backend decides how: the Airflow backend turns a spec into an operator, a
    local executor calls it in-process.
    """

    #: Unique within its motif. Backends use it as the task id.
    name: str
    #: A Python callable, called with the rendered ``params`` as keyword
    #: arguments, or the name of a backend action.
    action: Action
    #: Arguments for the action.
    params: Mapping[str, Any] = field(default_factory=dict)
    #: Names of the ``params`` rendered as templates before the task runs.
    templated: frozenset = frozenset()
    #: Capabilities the backend needs, on top of the ones implied by
    #: ``templated`` and ``retries`` (see ``required_capabilities``).
    requires: frozenset = frozenset()
    #: How many times to run the task again after a failure. ``None`` leaves
    #: it to the backend's default.
    retries: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _NAME_PATTERN.match(self.name):
            raise ValueError(
                f"Invalid task name {self.name!r}: use letters, digits, '_', '.'"
                " or '-', and do not start with '.' or '-'"
            )
        if isinstance(self.action, str):
            if not self.action:
                raise ValueError(f"Task {self.name!r}: the action name is empty")
        elif not callable(self.action):
            raise TypeError(
                f"Task {self.name!r}: the action must be a callable or an action"
                f" name, not {type(self.action).__name__}"
            )
        if self.retries is not None and (
            isinstance(self.retries, bool)
            or not isinstance(self.retries, int)
            or self.retries < 0
        ):
            raise ValueError(
                f"Task {self.name!r}: retries must be a non-negative integer"
            )

        # Copy and freeze the collections, so a spec cannot change after it is
        # built, even if the caller keeps a reference to what it passed in.
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))
        object.__setattr__(self, "templated", _frozenset_of_str(self.templated))
        object.__setattr__(self, "requires", _capabilities(self.requires))

        unknown = self.templated - self.params.keys()
        if unknown:
            raise ValueError(
                f"Task {self.name!r}: templated names are not params:"
                f" {', '.join(sorted(unknown))}"
            )

    @property
    def is_named_action(self) -> bool:
        """True when the action is a name the backend resolves."""
        return isinstance(self.action, str)

    @property
    def required_capabilities(self) -> frozenset:
        """Every capability the backend needs to run this task."""
        implied = set()
        if self.templated:
            implied.add(Capability.TEMPLATING)
        if self.retries:
            implied.add(Capability.RETRIES)
        return self.requires | implied

    def render_params(self, context: "RunContext") -> dict:
        """The params with the templated ones rendered by ``context``.

        Strings are rendered, and so are the strings inside lists, tuples and
        dicts, the same way Airflow renders templated fields.
        """
        return {
            key: (
                _render_value(value, context, self.name)
                if key in self.templated
                else value
            )
            for key, value in self.params.items()
        }


def validate_tasks(specs: Iterable[TaskSpec]) -> tuple:
    """Check a motif's specs and return them as a tuple.

    Names must be unique, because backends use them as task ids within the
    motif.
    """
    specs = tuple(specs)
    seen = set()
    for spec in specs:
        if not isinstance(spec, TaskSpec):
            raise TypeError(f"Expected a TaskSpec, got {type(spec).__name__}")
        if spec.name in seen:
            raise ValueError(f"Duplicate task name {spec.name!r}")
        seen.add(spec.name)
    return specs


def required_capabilities(specs: Iterable[TaskSpec]) -> frozenset:
    """Every capability needed to run all of ``specs``."""
    needed = frozenset()
    for spec in specs:
        needed |= spec.required_capabilities
    return needed


def _frozenset_of_str(values) -> frozenset:
    if isinstance(values, str):
        # A bare string would otherwise become a set of its characters.
        values = (values,)
    return frozenset(values)


def _capabilities(values) -> frozenset:
    if isinstance(values, (str, Capability)):
        values = (values,)
    return frozenset(Capability(value) for value in values)


def _render_value(value, context: "RunContext", task_name: str):
    if isinstance(value, str):
        return context.render(value, task_name=task_name)
    if isinstance(value, (list, tuple)):
        return type(value)(_render_value(item, context, task_name) for item in value)
    if isinstance(value, Mapping):
        return {
            key: _render_value(item, context, task_name) for key, item in value.items()
        }
    return value
