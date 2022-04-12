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


def data_partitioning_factory(data_partitioning):
    partitioning_type = data_partitioning['partitioning_type'].lower()
    mapping = {
        'time': BigQueryTimeDataPartitioning
    }
    output_cls = mapping.get(partitioning_type)
    if output_cls is None:
        raise TypeError(f'Format `{partitioning_type}` is not supported')
    return output_cls(**data_partitioning)


@dataclass(frozen=True)
class BigQueryDataIngestionMovementParameters(MovementParametersBase):
    extract_sql_query: str
    extract_connection_id: str
    data_partitioning: BigQueryDataPartitioning

    def __post_init__(self):
        data_partitioning = data_partitioning_factory(self.data_partitioning)
        # hack for frozen dataclass https://stackoverflow.com/a/54119384
        # overwriting data_partitioning with BigQueryDataPartitioning instance
        object.__setattr__(self, 'data_partitioning', data_partitioning)
