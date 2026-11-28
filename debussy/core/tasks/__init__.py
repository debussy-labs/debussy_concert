"""The engine-neutral task contract.

A motif describes what to run as ``TaskSpec`` objects; a backend decides how to
run them. Nothing here imports Airflow or any other engine.
"""

from debussy.core.tasks.capabilities import (
    Capability,
    UnsupportedCapabilityError,
    check_capabilities,
)
from debussy.core.tasks.context import RunContext
from debussy.core.tasks.protocols import PTaskMotif
from debussy.core.tasks.registry import ActionRegistry, UnknownActionError
from debussy.core.tasks.spec import (
    Action,
    TaskSpec,
    required_capabilities,
    result_of,
    validate_tasks,
)

__all__ = [
    "Action",
    "ActionRegistry",
    "Capability",
    "PTaskMotif",
    "RunContext",
    "TaskSpec",
    "UnknownActionError",
    "UnsupportedCapabilityError",
    "check_capabilities",
    "required_capabilities",
    "result_of",
    "validate_tasks",
]
