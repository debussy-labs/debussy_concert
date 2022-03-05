from typing import List
import warnings


class TableField:
    def __init__(self, name: str, description: str = '') -> None:
        self.name = name
        self.description = description

    def __str__(self):
        return self.name

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, str):
            return self.name == __o
        if isinstance(__o, TableField):
            return self.name == __o.name
        raise TypeError()

    def __hash__(self):
        return id(self)


class Table:
    def __init__(self, name: str,
                 fields: List[TableField]
                 ) -> None:
        self.name = name
        self.fields = fields

    @classmethod
    def load_from_dict(cls, table_data):
        return cls._load_from_dict(**table_data)

    @classmethod
    def _load_from_dict(cls, name,
                        fields):
        fields = [TableField(**field_data) for field_data in fields]

        return cls(name, fields)

    def __getitem__(self, key):
        """
            Called to implement evaluation of self[key]
            https://docs.python.org/3/reference/datamodel.html#object.__getitem__
        """
        warnings.warn("Getting property using dictionary access, please update to use class property",
                      category=FutureWarning)
        if not isinstance(key, str):
            raise TypeError('key must be strings')
        return getattr(self, key)
