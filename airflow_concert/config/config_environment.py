import os

from airflow_concert.config.config_base import ConfigBase


class ConfigEnvironment(ConfigBase):
    def __init__(
            self,
            project,
            region,
            zone,
            landing_bucket,
            raw_dataset,
            trusted_dataset
    ):
        self.project = project
        self.region = region
        self.zone = zone
        self.landing_bucket = landing_bucket
        self.raw_dataset = raw_dataset
        self.trusted_dataset = trusted_dataset
