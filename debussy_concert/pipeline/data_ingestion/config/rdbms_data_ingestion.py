from dataclasses import dataclass
from yaml_env_var_parser import load as yaml_load


from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.core.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.pipeline.data_ingestion.config.base import ConfigDataIngestionBase
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.rdbms_data_ingestion import (
    RdbmsDataIngestionMovementParameters,
)


@dataclass(frozen=True)
class ConfigRdbmsDataIngestion(ConfigDataIngestionBase):
    secret_manager_uri: str
    dataproc_config: dict

    @classmethod
    def load_from_file(cls, composition_config_file_path, env_file_path):

        env_config = ConfigEnvironment.load_from_file(env_file_path)

        with open(composition_config_file_path) as file:
            config = yaml_load(file)
        config["environment"] = env_config

        extract_movements = [
            RdbmsDataIngestionMovementParameters.load_from_dict(parameters)
            for parameters in config["ingestion_parameters"]
        ]
        del config["ingestion_parameters"]
        config["movements_parameters"] = extract_movements
        config["dag_parameters"] = ConfigDagParameters.create_from_dict(
            config["dag_parameters"]
        )

        # config dataproc (serverless or managed)
        if all(key in config for key in ("dataproc_config", "dataproc_serverless_config")):
            raise ValueError(
                "Use either dataproc_config or dataproc_serverless_config, not both!")

        if any(key in config for key in ("dataproc_config", "dataproc_serverless_config")):
            if config.get("dataproc_serverless_config") is not None:
                config["dataproc_config"] = config.pop(
                    "dataproc_serverless_config")
                config["dataproc_config"]["type"] = "serverless"
        else:
            raise ValueError(
                "Required dataproc_config (or dataproc_serverless_config) config parameter missing!")

        # config dataproc pyspark script
        if config["dataproc_config"].get("pyspark_script") is None:
            config["dataproc_config"]["pyspark_script"] = (
                f"gs://{env_config.artifact_bucket}"
                "/pyspark-scripts/jdbc-to-gcs/jdbc_to_gcs_hash_key.py"
            )

        return cls(**config)
