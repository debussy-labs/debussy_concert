from dataclasses import dataclass, asdict, field as dataclass_field
from typing import List, Optional
from yaml_env_var_parser import load as yaml_load

from debussy_concert.core.entities.table import Partitioning, Table, TableField, TableSchema


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
    def load_from_internal_table_field(cls, table_field: TableField):
        """
        Load data into BigQueryTableField class using TableField
        """
        fields_key = table_field.extra_options.get('fields')
        fields = None
        if fields_key:
            fields = []
            for inner_fields in fields_key:
                bq_field = cls.load_from_internal_table_field(
                    TableField(**inner_fields)
                )
                fields.append(bq_field)
        policy_tags = table_field.column_tags or []
        bq_policy_tags = BigQueryPolicyTags(names=policy_tags)
        field_schema = cls(
            name=table_field.name,
            description=table_field.description,
            type=table_field.data_type,
            mode=table_field.constraint,
            fields=fields,
            policy_tags=bq_policy_tags
        )
        return field_schema

    def get_field_schema(self):
        schema = asdict(self)
        return schema


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
    def load_from_partitioning(cls, partitioning: Partitioning):
        return cls(type=partitioning.granularity, field=partitioning.field)


def data_partitioning_factory(data_partitioning: Partitioning):
    partitioning_type = data_partitioning.type.lower()
    mapping = {"time": BigQueryTimePartitioning}
    output_cls = mapping.get(partitioning_type)
    if output_cls is None:
        raise TypeError(f"Format `{partitioning_type}` is not supported")
    return output_cls.load_from_partitioning(data_partitioning)


class BigQueryTableSchema(TableSchema):
    @classmethod
    def load_from_table_schema(cls, table_schema: TableSchema):
        fields = []
        for table_field in table_schema.fields:
            field = BigQueryTableField.load_from_internal_table_field(
                table_field
            )
            fields.append(field)
        return cls(fields=fields)


@dataclass
class BigQueryTable:
    schema: BigQueryTableSchema
    partitioning: BigQueryTimePartitioning

    @classmethod
    def load_from_table(cls, table: Table):
        schema = BigQueryTableSchema.load_from_table_schema(table.schema)
        partitioning = None
        if partitioning := table.partitioning:
            partitioning = data_partitioning_factory(partitioning)
        return cls(schema=schema, partitioning=partitioning)

    def as_dict(self):
        return asdict(self)
