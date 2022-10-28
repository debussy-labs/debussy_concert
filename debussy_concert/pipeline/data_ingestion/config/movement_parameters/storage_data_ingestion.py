from dataclasses import dataclass
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.time_partitioned import (
    BigQueryDataPartitioning
)
from debussy_concert.core.entities.table import BigQueryTable


@dataclass(frozen=True)
class SourceConfig:
    connection_id: str
    type: str
    uri: str
    is_dir: bool
    format: str


@dataclass(frozen=True)
class CsvFile(SourceConfig):
    field_delimiter: str
    print_header: bool = True


class ParquetFile(SourceConfig):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class AvroFile(SourceConfig):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("AvroFile is not implemented yet")


class JsonFile(SourceConfig):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("JsonFile is not implemented yet")


def source_factory(source_config):
    format: str = source_config["format"]
    mapping = {
        "csv": CsvFile,
        "parquet": ParquetFile,
        "avro": AvroFile,
        "json": JsonFile,
    }
    source_cls = mapping.get(format.lower())
    if source_cls is None:
        raise TypeError(f"Format `{format}` is not supported")
    return source_cls(**source_config)


@dataclass(frozen=True)
class StorageDataIngestionMovementParameters(
    MovementParametersBase
):
    name: str
    source_config: SourceConfig
    data_partitioning: BigQueryDataPartitioning
    raw_table_definition: str or BigQueryTable

    def __post_init__(self):
        source_config = source_factory(self.source_config)
        # hack for frozen dataclass https://stackoverflow.com/a/54119384
        # overwriting source_config with source_factory instance
        object.__setattr__(self, "source_config", source_config)

        if not isinstance(self.data_partitioning, BigQueryDataPartitioning):
            data_partitioning = BigQueryDataPartitioning(
                **self.data_partitioning)
            # hack for frozen dataclass https://stackoverflow.com/a/54119384
            # overwriting data_partitioning with BigQueryDataPartitioning instance
            object.__setattr__(self, "data_partitioning", data_partitioning)
        if self.raw_table_definition is not None:
            self.load_raw_table_definition_attr(
                self.raw_table_definition
            )

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

    @classmethod
    def load_from_dict(cls, movement_parameters):
        cls_instance = cls(**movement_parameters)
        return cls_instance
