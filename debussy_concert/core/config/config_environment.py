import os
from typing import Optional
from dataclasses import dataclass
from yaml_env_var_parser import load as yaml_load


@dataclass(frozen=True)
class ConfigEnvironment:
    project: str
    region: str
    zone: str
    artifact_bucket: str
    reverse_etl_bucket: str
    raw_vault_bucket: str
    staging_bucket: str
    raw_vault_dataset: str
    raw_dataset: str
    trusted_dataset: str
    reverse_etl_dataset: str
    temp_dataset: str
    data_lakehouse_connection_id: str
    landing_bucket: Optional[str] = None

    def __post_init__(self):
        # create env vars from the env config class with the debussy prefix
        # this is to make possible to use those var on compositions and movements
        prefix = "DEBUSSY_CONCERT"
        for key, value in self.__dict__.items():
            # dont create the env var if there is no value
            if value is None:
                continue

            env_key = f"{prefix}__{key}".upper()
            os.environ[env_key] = value

    @classmethod
    def load_from_file(cls, file_path):
        with open(file_path, "r") as file:
            env_config = yaml_load(file)
        return cls(**env_config)
