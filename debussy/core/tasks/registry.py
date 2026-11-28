"""Backend implementations of named actions."""

from typing import Callable, Dict, Generic, Optional, TypeVar

from debussy.core.tasks.spec import TaskSpec

Implementation = TypeVar("Implementation")


class UnknownActionError(LookupError):
    """No implementation is registered for an action name."""

    def __init__(self, backend: str, action: str) -> None:
        self.backend = backend
        self.action = action
        super().__init__(f"The {backend} backend has no action named {action!r}")


class ActionRegistry(Generic[Implementation]):
    """Maps action names to one backend's implementations.

    Each backend keeps its own registry, and decides what an implementation
    is: the Airflow backend registers operator factories, a local executor
    registers Python callables.
    """

    def __init__(self, backend: str) -> None:
        self.backend = backend
        self._actions: Dict[str, Implementation] = {}

    def register(
        self,
        name: str,
        implementation: Optional[Implementation] = None,
        *,
        replace: bool = False,
    ):
        """Register ``implementation`` under ``name``.

        Without ``implementation``, return a decorator that registers the
        decorated object. Registering a name twice raises ``ValueError``
        unless ``replace`` is true.
        """
        if not isinstance(name, str) or not name:
            raise ValueError("An action name must be a non-empty string")

        def decorator(target: Implementation) -> Implementation:
            if name in self._actions and not replace:
                raise ValueError(
                    f"The {self.backend} backend already has an action named {name!r}"
                )
            self._actions[name] = target
            return target

        if implementation is None:
            return decorator
        return decorator(implementation)

    def resolve(self, name: str) -> Implementation:
        try:
            return self._actions[name]
        except KeyError:
            raise UnknownActionError(self.backend, name) from None

    def resolve_spec(self, spec: TaskSpec) -> "Implementation | Callable":
        """The implementation of a named action, or the spec's own callable."""
        if spec.is_named_action:
            return self.resolve(spec.action)
        return spec.action

    def names(self) -> frozenset:
        return frozenset(self._actions)

    def __contains__(self, name: object) -> bool:
        return name in self._actions

    def __len__(self) -> int:
        return len(self._actions)
