from dataclasses import dataclass

from debussy_concert.core.config.movement_parameters.base import MovementParametersBase


@dataclass(frozen=True)
class ReverseEtlMovementParameters(MovementParametersBase):
    name: str
    reverse_etl_query: str
    reverse_etl_dataset_partition_type: str
    reverse_etl_dataset_partition_field: str
    extract_query_from_temp: str
    destination_type: str
    file_format: str
    field_delimiter: str
    destination_object_path: str
    gcp_connection_id: str
    destination_connection_id: str

    @classmethod
    def load_from_dict(cls, movement_parameters):
        return cls(**movement_parameters)
