"""What a backend can do, and what a task needs from it."""

from enum import Enum
from typing import Iterable


class Capability(str, Enum):
    """A feature a task may need from the backend that runs it."""

    #: Runs a pipeline on a schedule, without a person starting it.
    SCHEDULING = "scheduling"
    #: Runs a failed task again, ``TaskSpec.retries`` times.
    RETRIES = "retries"
    #: Renders the templated parameters of a task with a ``RunContext``.
    TEMPLATING = "templating"
    #: Runs an engine-specific operator, not only a Python callable.
    NATIVE_OPERATOR = "native_operator"
    #: Passes a task's return value to later tasks (``ti.xcom_pull`` in
    #: templates).
    XCOM = "xcom"


class UnsupportedCapabilityError(Exception):
    """A task needs capabilities the chosen backend does not have."""

    def __init__(self, subject: str, missing: Iterable[Capability]) -> None:
        self.subject = subject
        self.missing = frozenset(missing)
        names = ", ".join(sorted(capability.value for capability in self.missing))
        super().__init__(f"{subject} needs capabilities the backend lacks: {names}")


def check_capabilities(
    required: Iterable[Capability],
    provided: Iterable[Capability],
    *,
    subject: str,
) -> None:
    """Raise ``UnsupportedCapabilityError`` if ``provided`` misses any of
    ``required``. ``subject`` names what needs them, for the error message."""
    missing = frozenset(required) - frozenset(provided)
    if missing:
        raise UnsupportedCapabilityError(subject, missing)
