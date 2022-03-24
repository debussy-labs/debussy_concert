from typing import List
import yaml

from debussy_concert.config.config_environment import ConfigEnvironment
from debussy_concert.config.config_composition import ConfigComposition
from debussy_concert.config.movement_parameters.data_ingestion import DataIngestionMovementParameters


class ConfigDataIngestion(ConfigComposition):
    def __init__(
        self,
        name,
        description,
        database,
        secret_id,
        dag_parameters,
        environment: ConfigEnvironment,
        ingestion_parameters: List[DataIngestionMovementParameters],
        rdbms_name,
        dataproc_config=None,
    ):
        super().__init__(name=name, description=description, movements_parameters=ingestion_parameters,
                         environment=environment, dag_parameters=dag_parameters)
        self.database = database
        self.secret_id = secret_id
        self.rdbms_name = rdbms_name
        self.dataproc_config = dataproc_config
        self.table_prefix = database.lower()

    @classmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):

        env_config = ConfigEnvironment.load_from_file(env_file_path)

        with open(composition_config_file_path) as file:
            config = yaml.safe_load(file)
        config["environment"] = env_config
        extract_movements = [DataIngestionMovementParameters.load_from_dict(parameters)
                             for parameters in config["ingestion_parameters"]]
        config["ingestion_parameters"] = extract_movements
        return cls(**config)
