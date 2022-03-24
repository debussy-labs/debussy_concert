from typing import TypeVar


class MovementParametersBase:
    def __init__(self, name):
        self.name = name


MovementParametersType = TypeVar('MovementParametersType', bound=MovementParametersBase)
