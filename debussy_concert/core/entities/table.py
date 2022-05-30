from typing import Dict, List, Optional


class SchemaField:
    def __init__(self, name: str, data_type: str, constraint=None, description: str = '', column_tags: list = None,
                 extra_options: Optional[Dict] = None) -> None:
        self.name = name
        self.data_type = data_type
        self.constraint = constraint
        self.description = description
        self.column_tags = column_tags
        self.extra_options = extra_options or {}

    def __str__(self):
        return self.name

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, str):
            return self.name == __o
        if isinstance(__o, SchemaField):
            return self.name == __o.name
        raise TypeError()

    def __hash__(self):
        return id(self)


class Table:

    def __init__(self, name, table_fields: List[SchemaField]):
        self.name = name
        self.schema = table_fields
