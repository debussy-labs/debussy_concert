import yaml

from airflow_concert.config.config_environment import ConfigEnvironment
from airflow_concert.config.config_dag_parameters import ConfigDagParameters
from airflow_concert.config.config_base import ConfigBase


class ConfigIntegration(ConfigBase):
    def __init__(
        self,
        name,
        description,
        database,
        secret_id,
        dag_parameters,
        environment: ConfigEnvironment,
        tables,
        dataproc_config=None,
    ):
        self.name = name
        self.database = database
        self.secret_id = secret_id
        self.description = description
        self.environment = environment
        self.tables = tables
        self.dag_parameters = ConfigDagParameters.create_from_dict(dag_parameters)
        self.dataproc_config = dataproc_config

    @classmethod
    def load_from_file(cls, integration_file_path, env_file_path):

        env_config = ConfigEnvironment.load_from_file(env_file_path)

        with open(integration_file_path) as file:
            config = yaml.safe_load(file)
        config["environment"] = env_config

        return cls(**config)
