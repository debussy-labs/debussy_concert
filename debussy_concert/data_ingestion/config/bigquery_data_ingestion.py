from dataclasses import dataclass
import yaml

from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.data_ingestion.config.base import ConfigDataIngestionBase
from debussy_concert.core.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.data_ingestion.config.movement_parameters.bigquery import BigQueryDataIngestionMovementParameters


@dataclass(frozen=True)
class ConfigBigQueryDataIngestion(ConfigDataIngestionBase):
    gcp_connection_id: str

    @classmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):

        env_config = ConfigEnvironment.load_from_file(env_file_path)

        with open(composition_config_file_path) as file:
            config = yaml.safe_load(file)
        config["environment"] = env_config
        extract_movements = [BigQueryDataIngestionMovementParameters(**parameters)
                             for parameters in config["ingestion_parameters"]]
        del config["ingestion_parameters"]
        config["movements_parameters"] = extract_movements
        config["dag_parameters"] = ConfigDagParameters.create_from_dict(config["dag_parameters"])
        return cls(**config)
