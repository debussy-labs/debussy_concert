from dataclasses import dataclass
from debussy_concert.data_ingestion.config.movement_parameters.time_partitioned import TimePartitionedDataIngestionMovementParameters


@dataclass(frozen=True)
class RdbmsDataIngestionMovementParameters(TimePartitionedDataIngestionMovementParameters):
    extraction_query: str

    @classmethod
    def load_from_dict(cls, movement_data):
        return cls(**movement_data)
