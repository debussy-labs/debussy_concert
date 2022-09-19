from typing import List
from yaml_env_var_parser import load as yaml_load

from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.core.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.pipeline.reverse_etl.config.movement_parameters.reverse_etl import (
    ReverseEtlMovementParameters,
)


class ConfigReverseEtl(ConfigComposition):
    def __init__(
        self,
        name,
        description,
        dag_parameters,
        extraction_movements: List[ReverseEtlMovementParameters],
        environment: ConfigEnvironment,
    ):
        super().__init__(
            name=name,
            description=description,
            movements_parameters=extraction_movements,
            environment=environment,
            dag_parameters=dag_parameters,
        )

    @classmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):

        env_config = ConfigEnvironment.load_from_file(env_file_path)

        with open(composition_config_file_path) as file:
            config = yaml_load(file)
        config["environment"] = env_config
        extract_movements = [
            ReverseEtlMovementParameters.load_from_dict(parameters)
            for parameters in config["extraction_movements"]
        ]
        config["extraction_movements"] = extract_movements
        config["dag_parameters"] = ConfigDagParameters.create_from_dict(
            config["dag_parameters"]
        )

        return cls(**config)
