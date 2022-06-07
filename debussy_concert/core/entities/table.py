from dataclasses import dataclass, asdict
from typing import List, Optional
import yaml


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


@dataclass
class Table:
    fields: List[TableField]

    @classmethod
    def load_from_dict(cls, table_dict):
        fields = []
        for field_dict in table_dict['fields']:
            field = TableField(**field_dict)
            fields.append(field)
        return cls(fields=fields)

    @classmethod
    def load_from_file(cls, file_path):
        with open(file_path) as file:
            table_dict = yaml.safe_load(file)
        return cls.load_from_dict(table_dict)


class BigQueryTable(Table):
    @classmethod
    def load_from_dict(cls, table_dict):
        fields = []
        for field_dict in table_dict['fields']:
            field = BigQueryTableField.load_from_internal_table_field_interface_dict(field_dict)
            fields.append(field)
        return cls(fields=fields)
