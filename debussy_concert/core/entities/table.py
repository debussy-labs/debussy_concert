from dataclasses import dataclass, asdict
from typing import Any, List, Optional
import os
import yaml

from debussy_concert.core.service.debussy_yaml_safe_loader import DebussyYamlSafeLoader


class TableField:
    def __init__(
            self,
            name: str,
            data_type: str,
            constraint: Optional[str] = None,
            description: Optional[str] = None,
            column_tags: Optional[List] = None,
            **extra_options
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
    mode: str = 'NULLABLE'
    fields: Optional[List['BigQueryTableField']] = None

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
        fields_key = field_dict.get('fields')
        fields = None
        if fields_key:
            fields = []
            for inner_fields in fields_key:
                bq_field = cls.load_from_internal_table_field_interface_dict(inner_fields)
                fields.append(bq_field)
        field_schema = cls(
            name=field_dict['name'],
            description=field_dict.get('description'),
            type=field_dict['data_type'],
            mode=field_dict.get('constraint'),
            fields=fields
        )
        return field_schema

    def get_field_schema(self):
        return asdict(self)


class Partitioning:
    granularity: str
    field: str


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

    def __init__(self, type: str, expiration_ms: Optional[str] = None, field: Optional[str] = None):
        _type = type.upper()
        if _type not in ('DAY', 'HOUR', 'MONTH', 'YEAR'):
            raise ValueError(f"Invalid type: {type}")
        self.type = _type
        self.expiration_ms = expiration_ms
        self.field = field

    def to_dict(self) -> dict:
        ret = {
            "type": self.type,
            "expirationMs": self.expiration_ms,
            "field": self.field,
        }
        return ret

    @classmethod
    def load_from_internal_partitioning_interface_dict(cls, data_dict):
        del data_dict['type']
        return cls(
            type=data_dict['granularity'],
            field=data_dict['field']
        )


@dataclass
class Table:
    fields: List[TableField]
    partitioning: Any

    @classmethod
    def load_from_dict(cls, table_dict):
        fields = []
        for field_dict in table_dict['fields']:
            field = TableField(**field_dict)
            fields.append(field)
        return cls(fields=fields)

    @classmethod
    def load_from_file(cls, file_path: str):
        with open(file_path) as file:
            table_dict = yaml.load(file, Loader=DebussyYamlSafeLoader)
        return cls.load_from_dict(table_dict)


def data_partitioning_factory(data_partitioning):
    partitioning_type = data_partitioning['type'].lower()
    mapping = {
        'time': BigQueryTimePartitioning
    }
    output_cls = mapping.get(partitioning_type)
    if output_cls is None:
        raise TypeError(f'Format `{partitioning_type}` is not supported')
    return output_cls.load_from_internal_partitioning_interface_dict(data_partitioning)


class BigQueryTable(Table):
    partitioning: BigQueryTimePartitioning

    @classmethod
    def load_from_dict(cls, table_dict):
        fields = []
        for field_dict in table_dict['fields']:
            field = BigQueryTableField.load_from_internal_table_field_interface_dict(field_dict)
            fields.append(field)
        partitioning = None
        if partitioning_dict := table_dict.get('partitioning'):
            partitioning = data_partitioning_factory(partitioning_dict)
        return cls(fields=fields, partitioning=partitioning)
