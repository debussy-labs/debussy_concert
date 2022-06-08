from typing import List, Optional
from dataclasses import dataclass, field as dataclass_field
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
class RdbmsDataIngestionMovementParameters(MovementParametersBase):
    
    primary_key: Optional[TableField] = None    
    data_tracking_tag: Optional[str] = None
    extraction_query: Optional[str] = dataclass_field(default=None, init=False, repr=False)

    def __post_init__(self):
        pii_columns_joined = self.pii_columns_join()
        # hack for frozen dataclass https://stackoverflow.com/a/54119384
        # overwriting pii_columns with pii_columns_joined
        object.__setattr__(self, 'pii_columns', pii_columns_joined)
        object.__setattr__(self, 'extraction_query', self.get_extraction_query())

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

    def get_extraction_query(self):
        fields = [f"`{field}`" for field in self.fields]
        fields = ", ".join(fields)

        query = (
            f"SELECT {fields} FROM {self.name} "
            f"WHERE {self.offset_field} > '{{{{ prev_execution_date }}}}' AND {self.offset_field} <= '{{{{ execution_date }}}}'"
        )
        return self.extraction_query
