from dataclasses import dataclass
import yaml


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

    @classmethod
    def load_from_file(cls, file_path):
        with open(file_path, 'r') as file:
            env_config = yaml.safe_load(file)
        return cls(**env_config)
