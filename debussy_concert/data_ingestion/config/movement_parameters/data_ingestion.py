from typing import List, Optional
from dataclasses import dataclass
import warnings
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase


class TableField:
    def __init__(self, name: str, description: str = '', is_pii: bool = False) -> None:
        self.name = name
        self.description = description
        self.is_pii = is_pii

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


@dataclass(frozen=True)
class DataIngestionMovementParameters(MovementParametersBase):
    fields: List[TableField]
    primary_key: Optional[TableField] = None
    pii_columns: Optional[List[TableField]] = None
    data_tracking_tag: Optional[str] = None
    offset_type: Optional[str] = None
    offset_field: Optional[TableField] = None
    business_partition_column: Optional[TableField] = None

    def __post_init__(self):
        pii_columns_joined = self.pii_columns_join()
        # hack for frozen dataclass https://stackoverflow.com/a/54119384
        # overwriting pii_columns with pii_columns_joined
        object.__setattr__(self, 'pii_columns', pii_columns_joined)

    def pii_columns_join(self):
        # pii column can be set both on pii_columns argument or on a flag in field
        # this function join those two methods
        pii_columns = self.pii_columns or []
        for field in self.fields:
            if field.is_pii:
                pii_columns.append(field)
        # cast to set to remove duplicates fields
        print(pii_columns)
        pii_columns = set(pii_columns)
        return pii_columns

    def pii_columns_names(self) -> List[str]:
        return [column.name for column in self.pii_columns]

    # alias
    pii_fields: List[TableField] = pii_columns
    pii_fields_names = pii_columns_names

    @classmethod
    def load_from_dict(cls, table_data):
        return cls._load_from_dict(**table_data)

    @classmethod
    def _load_from_dict(cls, name,
                        fields,
                        primary_key=None,
                        pii_columns=None,
                        data_tracking_tag=None,
                        offset_type=None,
                        offset_field=None,
                        business_partition_column=None):
        # fields = [TableField(name, **field_data) for name, field_data in fields.items()]
        fields = [TableField(**field_data) for field_data in fields]

        if primary_key is not None:
            # primary_key = list(filter(is_field_name_equal(primary_key), fields))
            primary_key = [field for field in fields if field.name == primary_key]
            assert len(primary_key) == 1
            primary_key = primary_key[0]
        if pii_columns is not None:
            pii_columns_names = pii_columns.split(",")
            pii_columns = [field for field in fields if field.name in pii_columns_names]
        if offset_field is not None:
            offset_field = [field for field in fields if field.name == offset_field]
            assert len(offset_field) == 1
            offset_field = offset_field[0]
        if business_partition_column is not None:
            business_partition_column = [
                field for field in fields if field.name == business_partition_column]
            assert len(business_partition_column) == 1
            business_partition_column = business_partition_column[0]
        return cls(name, fields, primary_key, pii_columns, data_tracking_tag,
                   offset_type, offset_field, business_partition_column)

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
