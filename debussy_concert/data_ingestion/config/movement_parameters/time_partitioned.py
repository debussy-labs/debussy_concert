from typing import Optional
from dataclasses import dataclass
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase
from debussy_concert.core.entities.table import BigQueryTable


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
class TimePartitionedDataIngestionMovementParameters(MovementParametersBase):
    extract_connection_id: str
    data_partitioning: BigQueryTimeDataPartitioning
    raw_table_definition: str

    def __post_init__(self):
        if not isinstance(self.data_partitioning, BigQueryTimeDataPartitioning):
            data_partitioning = data_partitioning_factory(self.data_partitioning)
            # hack for frozen dataclass https://stackoverflow.com/a/54119384
            # overwriting data_partitioning with BigQueryTimeDataPartitioning instance
            object.__setattr__(self, 'data_partitioning', data_partitioning)
        if self.raw_table_definition is not None:
            self.load_raw_table_definition_attr(self.raw_table_definition)

    def load_raw_table_definition_attr(self, raw_table_definition):
        if isinstance(raw_table_definition, str):
            raw_table_definition = BigQueryTable.load_from_file(self.raw_table_definition)
        elif isinstance(raw_table_definition, dict):
            raw_table_definition = BigQueryTable.load_from_dict(self.raw_table_definition)
        else:
            raise TypeError("Invalid type for raw_table_definition.")
        object.__setattr__(self, 'raw_table_definition', raw_table_definition)
