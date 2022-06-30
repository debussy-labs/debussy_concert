from abc import ABC
from dataclasses import dataclass
from typing import List

from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.core.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase


@dataclass(frozen=True)
class ConfigComposition(ABC):
    name: str
    description: str
    movements_parameters: List[MovementParametersBase]
    environment: ConfigEnvironment
    dag_parameters: ConfigDagParameters

    def load_from_file(cls, composition_config_file_path, env_file_path):
        raise NotImplementedError()
