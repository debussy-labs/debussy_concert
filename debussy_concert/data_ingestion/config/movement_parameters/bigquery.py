from dataclasses import dataclass
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase


@dataclass(frozen=True)
class BigQueryDataIngestionMovementParameters(MovementParametersBase):
    extract_sql_query: str
    output_config: dict
