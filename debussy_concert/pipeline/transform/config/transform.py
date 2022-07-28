from yaml_env_var_parser import load as yaml_load

from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.core.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.pipeline.transform.config.movement_parameters.dbt import DbtMovementParameters, DbtParameters


class ConfigTransformComposition(ConfigComposition):

    @classmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):

        env_config = ConfigEnvironment.load_from_file(env_file_path)

        with open(composition_config_file_path) as file:
            config = yaml_load(file)
        config["environment"] = env_config
        movements_parameters = []
        for movement_parameters_dict in config["transformation_parameters"]:
            dbt_run_parameters = movement_parameters_dict['dbt_run_parameters']
            dbt_run_param = DbtParameters(**dbt_run_parameters)
            movement_parameter = DbtMovementParameters(dbt_run_parameters=dbt_run_param)
            movements_parameters.append(movement_parameter)
        del config["transformation_parameters"]
        config["movements_parameters"] = movements_parameters
        config["dag_parameters"] = ConfigDagParameters.create_from_dict(config["dag_parameters"])
        return cls(**config)
