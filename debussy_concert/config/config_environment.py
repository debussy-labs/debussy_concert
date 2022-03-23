import os

from debussy_concert.config.config_base import ConfigBase


class ConfigEnvironment(ConfigBase):
    def __init__(
            self,
            project,
            region,
            zone,
            artifact_bucket,
            reverse_etl_bucket,
            landing_bucket,
            staging_bucket,
            landing_dataset,
            raw_dataset,
            trusted_dataset,
            reverse_etl_dataset,
            temp_dataset
    ):
        self.project = project
        self.region = region
        self.zone = zone
        self.reverse_etl_bucket = reverse_etl_bucket
        self.landing_bucket = landing_bucket
        self.landing_dataset = landing_dataset
        self.staging_bucket = staging_bucket
        self.artifact_bucket = artifact_bucket
        self.raw_dataset = raw_dataset
        self.trusted_dataset = trusted_dataset
        self.reverse_etl_dataset = reverse_etl_dataset
        self.temp_dataset = temp_dataset
