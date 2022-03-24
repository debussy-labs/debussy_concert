from abc import abstractclassmethod
from typing import List

from debussy_concert.config.config_environment import ConfigEnvironment
from debussy_concert.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.config.movement_parameters.base import MovementParametersBase
from debussy_concert.config.config_base import ConfigBase


class ConfigComposition(ConfigBase):
    def __init__(
        self,
        name,
        description,
        movements_parameters: List[MovementParametersBase],
        environment: ConfigEnvironment,
        dag_parameters
    ):
        self.name = name
        self.description = description
        self.movements_parameters = movements_parameters
        self.environment = environment
        self.dag_parameters = ConfigDagParameters.create_from_dict(dag_parameters)

    @abstractclassmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):
        pass
