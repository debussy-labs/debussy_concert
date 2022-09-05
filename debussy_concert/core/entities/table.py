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
        **extra_options,
    ) -> None:
        self.name = name
        self.data_type = data_type
        self.constraint = constraint
        self.description = description
        self.column_tags = column_tags
        self.extra_options = extra_options or {}

    def get_field_schema(self):
        raise NotImplementedError


@dataclass
class BigQueryPolicyTags:
    names: List[str]


@dataclass
class BigQueryTableField:
    """
    ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables?hl=pt-br#TableFieldSchema
    {
    "name": string,
    "description": string,
    "type": string,
    "mode": string,
    "fields": [
        {
        object (TableFieldSchema)
        }
    ],
    "policyTags": {
        "names": [
        string
        ]
    },
    "maxLength": string,
    "precision": string,
    "scale": string,
    "collation": string
    }
    """

    name: str
    description: str
    type: str
    mode: str = "NULLABLE"
    fields: Optional[List["BigQueryTableField"]] = None
    policy_tags: Optional[BigQueryPolicyTags] = dataclass_field(
        default=BigQueryPolicyTags([])
    )

    def __post_init__(self):
        # those should be upper case
        self.type = self.type.upper()
        if self.mode is not None:
            self.mode = self.mode.upper()

    @classmethod
    def load_from_internal_table_field_interface_dict(cls, field_dict):
        """
        Load data into BigQueryTableField class using TableField interface
        """
        fields_key = field_dict.get("fields")
        fields = None
        if fields_key:
            fields = []
            for inner_fields in fields_key:
                bq_field = cls.load_from_internal_table_field_interface_dict(
                    inner_fields
                )
                fields.append(bq_field)
        policy_tags = field_dict.get("tags", [])
        bq_policy_tags = BigQueryPolicyTags(names=policy_tags)
        field_schema = cls(
            name=field_dict["name"],
            description=field_dict.get("description"),
            type=field_dict["data_type"],
            mode=field_dict.get("constraint"),
            fields=fields,
            policy_tags=bq_policy_tags,
        )
        return field_schema

    def get_field_schema(self):
        schema = asdict(self)
        return schema


class Partitioning:
    granularity: str
    field: str


@dataclass
class BigQueryTimePartitioning:
    """
    NOTE: might exist an implementation for this in the google.cloud.bigquery sdk, i could not find it
    https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timepartitioning
    {
       "type": string,
       "expirationMs": string,
       "field": string,
       "requirePartitionFilter": boolean # deprecated
    }
    """

    type: str
    expiration_ms: Optional[str] = None
    field: Optional[str] = None

    def __post_init__(self):
        type_ = self.type.upper()
        if type_ not in ("DAY", "HOUR", "MONTH", "YEAR"):
            raise ValueError(f"Invalid type: {type}")
        self.type = type_

    def to_dict(self) -> dict:
        ret = {
            "type": self.type,
            "expirationMs": self.expiration_ms,
            "field": self.field,
        }
        return ret

    @classmethod
    def load_from_internal_partitioning_interface_dict(cls, data_dict):
        del data_dict["type"]
        return cls(type=data_dict["granularity"], field=data_dict["field"])


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


def data_partitioning_factory(data_partitioning):
    partitioning_type = data_partitioning["type"].lower()
    mapping = {"time": BigQueryTimePartitioning}
    output_cls = mapping.get(partitioning_type)
    if output_cls is None:
        raise TypeError(f"Format `{partitioning_type}` is not supported")
    return output_cls.load_from_internal_partitioning_interface_dict(data_partitioning)


class BigQueryTableSchema(TableSchema):
    @classmethod
    def load_from_dict(cls, table_dict):
        fields = []
        for field_dict in table_dict["fields"]:
            field = BigQueryTableField.load_from_internal_table_field_interface_dict(
                field_dict
            )
            fields.append(field)
        return cls(fields=fields)


@dataclass
class BigQueryTable:
    schema: BigQueryTableSchema
    partitioning: BigQueryTimePartitioning

    @classmethod
    def load_from_dict(cls, table_dict):
        print(table_dict)
        schema = BigQueryTableSchema.load_from_dict(table_dict)
        partitioning = None
        if partitioning_dict := table_dict.get("partitioning"):
            partitioning = data_partitioning_factory(partitioning_dict)
        return cls(schema=schema, partitioning=partitioning)

    @classmethod
    def load_from_file(cls, file_path: str):
        with open(file_path) as file:
            table_dict = yaml_load(file)
        return cls.load_from_dict(table_dict)

    def as_dict(self):
        return asdict(self)
