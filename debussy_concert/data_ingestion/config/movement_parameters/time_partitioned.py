from dataclasses import dataclass
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase


@dataclass(frozen=True)
class BigQueryDataPartitioning:
    partitioning_type: str
    gcs_partition_schema: str
    partition_field: str
    destination_partition: str


@dataclass(frozen=True)
class BigQueryTimeDataPartitioning(BigQueryDataPartitioning):
    partition_granularity: str


@dataclass(frozen=True)
class TimePartitionedDataIngestionMovementParameters(MovementParametersBase):
    extract_connection_id: str
    data_partitioning: BigQueryTimeDataPartitioning

    def __post_init__(self):
        if isinstance(self.data_partitioning, BigQueryTimeDataPartitioning):
            return
        data_partitioning = BigQueryTimeDataPartitioning(**self.data_partitioning)
        # hack for frozen dataclass https://stackoverflow.com/a/54119384
        # overwriting data_partitioning with BigQueryTimeDataPartitioning instance
        object.__setattr__(self, 'data_partitioning', data_partitioning)
