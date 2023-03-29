from dataclasses import dataclass, asdict, field as dataclass_field
from typing import List, Optional


from yaml_env_var_parser import load as yaml_load


class TableField:
    def __init__(
        self,
        name: str,
        data_type: str,
        constraint: Optional[str] = None,
        description: Optional[str] = None,
        column_tags: Optional[List] = None,
        is_metadata: Optional[bool] = False,
        **extra_options,
    ) -> None:
        self.name = name
        self.data_type = data_type
        self.constraint = constraint
        self.description = description
        self.column_tags = column_tags
        self.is_metadata = is_metadata
        self.extra_options = extra_options or {}

    def __repr__(self):
        return str(self.__dict__)

    def get_field_schema(self):
        raise NotImplementedError(
            "This is a generic table field and must be converted to a specific technology schema")


class Partitioning:
    type: str
    granularity: str
    field: str


@dataclass
class TableSchema:
    fields: List[TableField]

    @classmethod
    def load_from_dict(cls, table_dict):
        fields = []
        for field_dict in table_dict["fields"]:
            field = TableField(**field_dict)
            fields.append(field)
        return cls(fields=fields)

    def as_dict(self):
        return asdict(self)


@dataclass
class Table:
    schema: TableSchema
    partitioning: Partitioning

    @classmethod
    def load_from_file(cls, file_path: str):
        with open(file_path) as file:
            table_dict = yaml_load(file)
        return cls.load_from_dict(table_dict)

    @classmethod
    def load_from_dict(cls, table_dict):
        schema = TableSchema.load_from_dict(table_dict)
        partitioning = None
        if partitioning_dict := table_dict.get("partitioning"):
            partitioning = Partitioning(**partitioning_dict)
        return cls(schema=schema, partitioning=partitioning)
