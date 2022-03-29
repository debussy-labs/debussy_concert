from abc import abstractclassmethod
from dataclasses import dataclass
from typing import List

from debussy_concert.config.config_environment import ConfigEnvironment
from debussy_concert.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.config.movement_parameters.base import MovementParametersBase


@dataclass(frozen=True)
class ConfigComposition:
    name: str
    description: str
    movements_parameters: List[MovementParametersBase]
    environment: ConfigEnvironment
    dag_parameters: ConfigDagParameters

    @abstractclassmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):
        pass
