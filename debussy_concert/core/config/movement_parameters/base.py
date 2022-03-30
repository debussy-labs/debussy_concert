from dataclasses import dataclass
#import json
from typing import TypeVar


@dataclass(frozen=True)
class MovementParametersBase:
    name: str


MovementParametersType = TypeVar('MovementParametersType', bound=MovementParametersBase)
