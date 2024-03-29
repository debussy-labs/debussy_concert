from dataclasses import dataclass
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.time_partitioned import (
    TimePartitionedDataIngestionMovementParameters,
)


@dataclass(frozen=True)
class BigQueryDataIngestionMovementParameters(
    TimePartitionedDataIngestionMovementParameters
):
    extraction_query: str
