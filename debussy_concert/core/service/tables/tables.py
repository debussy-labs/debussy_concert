from typing import List, Iterator, Dict
from debussy_concert.core.entities.table import TableSchema


class TablesService:
    def __init__(self, tables: List[TableSchema] = None) -> None:
        self._tables = tables or []

    def tables_names(self) -> Iterator[str]:
        for table in self.tables():
            yield table.name

    def tables(self) -> Iterator[TableSchema]:
        for table in self._tables:
            yield table

    def add_table(self, table: TableSchema):
        self._tables.append(table)

    def add_table_from_dict(self, table_data: Dict):
        table = TableSchema.load_from_dict(table_data)
        self.add_table(table)

    def add_tables_from_dict(self, tables_data_list: List[Dict]):
        for table_data in tables_data_list:
            self.add_table_from_dict(table_data)

    @classmethod
    def create_from_dict(cls, tables_data_list: List[Dict]):
        tables = []
        for table_data in tables_data_list:
            table = TableSchema.load_from_dict(table_data)
            tables.append(table)
        return cls(tables=tables)
