from abc import ABC
from typing import List
from dataclasses import dataclass

from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.data_ingestion.config.movement_parameters.time_partitioned import TimePartitionedDataIngestionMovementParameters


@dataclass(frozen=True)
class ConfigDataIngestionBase(ConfigComposition, ABC):
    # ingestion are time partitioned on bigquery based on ingestion logical date,
    # even if it is only one partition
    movements_parameters: List[TimePartitionedDataIngestionMovementParameters]
    # source name like dataset name users, customers, sales etc
    source_name: str
    # source tech type like mysql, mssql, bigquery, api, gcs
    source_type: str

    @property
    def table_prefix(self):
        return self.source_type.lower()
