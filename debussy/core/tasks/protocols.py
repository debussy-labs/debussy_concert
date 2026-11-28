from typing import Protocol, Sequence, runtime_checkable

from debussy.core.tasks.spec import TaskSpec


@runtime_checkable
class PTaskMotif(Protocol):
    """A motif that describes its work as engine-neutral tasks.

    Any backend can run such a motif. A motif that only has ``build()`` builds
    Airflow tasks directly, so only the Airflow backend can run it.
    """

    name: str

    def tasks(self) -> Sequence[TaskSpec]:
        """The motif's tasks, in the order they run."""
        ...
