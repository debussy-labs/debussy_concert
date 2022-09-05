from dataclasses import dataclass
from typing import TypeVar


@dataclass(frozen=True)
class MovementParametersBase:
    name: str


MovementParametersType = TypeVar("MovementParametersType", bound=MovementParametersBase)
