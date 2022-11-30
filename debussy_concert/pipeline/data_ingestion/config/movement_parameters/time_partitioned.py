from dataclasses import dataclass
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase
from debussy_concert.core.entities.table import BigQueryTable


@dataclass(frozen=True)
class BigQueryDataPartitioning:
    gcs_partition_schema: str
    destination_partition: str


@dataclass(frozen=True)
class TimePartitionedDataIngestionMovementParameters(MovementParametersBase):
    extract_connection_id: str
    data_partitioning: BigQueryDataPartitioning
    raw_table_definition: str or BigQueryTable

    def __post_init__(self):
        if not isinstance(self.data_partitioning, BigQueryDataPartitioning):
            data_partitioning = BigQueryDataPartitioning(
                **self.data_partitioning)
            # hack for frozen dataclass https://stackoverflow.com/a/54119384
            # overwriting data_partitioning with BigQueryDataPartitioning instance
            object.__setattr__(self, "data_partitioning", data_partitioning)
        if self.raw_table_definition is not None:
            self.load_raw_table_definition_attr(self.raw_table_definition)

    def load_raw_table_definition_attr(self, raw_table_definition):
        if isinstance(raw_table_definition, str):
            raw_table_definition = BigQueryTable.load_from_file(
                self.raw_table_definition
            )
        elif isinstance(raw_table_definition, dict):
            raw_table_definition = BigQueryTable.load_from_dict(
                self.raw_table_definition
            )
        else:
            raise TypeError("Invalid type for raw_table_definition.")
        object.__setattr__(self, "raw_table_definition", raw_table_definition)
