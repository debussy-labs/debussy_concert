import yaml

from airflow_concert.config.config_environment import ConfigEnvironment
from airflow_concert.config.config_dag_parameters import ConfigDagParameters
from airflow_concert.config.config_base import ConfigBase


class ConfigReverseEtl(ConfigBase):
    def __init__(
        self,
        name,
        description,
        dag_parameters,
        tables,
        environment: ConfigEnvironment
    ):
        self.name = name
        self.description = description
        self.environment = environment
        self.dag_parameters = ConfigDagParameters.create_from_dict(dag_parameters)
        self.tables = tables

    @classmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):

        env_config = ConfigEnvironment.load_from_file(env_file_path)

        with open(composition_config_file_path) as file:
            config = yaml.safe_load(file)
        config["environment"] = env_config

        return cls(**config)
