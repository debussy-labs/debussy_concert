from debussy_concert.config.movement_parameters.base import MovementParametersBase


class ReverseEtlMovementParameters(MovementParametersBase):
    def __init__(self, name,
                 reverse_etl_query,
                 reverse_etl_dataset_partition_type,
                 reverse_etl_dataset_partition_field,
                 extract_query_from_temp,
                 destination_type,
                 file_format,
                 field_delimiter,
                 destination_object_path,
                 bigquery_connection_id,
                 destination_connection_id):
        super().__init__(name)
        self.reverse_etl_query = reverse_etl_query
        self.reverse_etl_dataset_partition_type = reverse_etl_dataset_partition_type
        self.reverse_etl_dataset_partition_field = reverse_etl_dataset_partition_field
        self.extract_query_from_temp = extract_query_from_temp
        self.destination_type = destination_type
        self.file_format = file_format
        self.field_delimiter = field_delimiter
        self.destination_object_path = destination_object_path
        self.bigquery_connection_id = bigquery_connection_id
        self.destination_connection_id = destination_connection_id

    @classmethod
    def load_from_dict(cls, movement_parameters):
        return cls(**movement_parameters)
