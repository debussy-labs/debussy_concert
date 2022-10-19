from debussy_concert.core.entities.table import Partitioning, Table, TableField
from debussy_concert.core.entities.bigquery_table import BigQueryTable, BigQueryTableField, BigQueryTimePartitioning


class GoogleCloudLakeHouseService:
    @staticmethod
    def convert_table(table: Table) -> BigQueryTable:
        raise NotImplementedError("TBD")

    @staticmethod
    def convert_partitioning(partitioning: Partitioning) -> BigQueryTimePartitioning:
        raise NotImplementedError("TBD")

    @staticmethod
    def convert_table_field(table: TableField) -> BigQueryTableField:
        bq_field = BigQueryTableField.load_from_internal_table_field(table)
        return bq_field

    @staticmethod
    def get_table_schema_without_metadata_columns(table: Table) -> dict:
        table_fields = table.schema.fields
        table_schema = []
        for field in table_fields:
            if field.is_metadata:
                continue
            field = GoogleCloudLakeHouseService.convert_table_field(field)
            table_schema.append(field.get_field_schema())
        return table_schema

    @staticmethod
    def get_table_schema(table: Table) -> dict:
        table_fields = table.schema.fields
        table_schema = []
        for field in table_fields:
            field = GoogleCloudLakeHouseService.convert_table_field(field)
            table_schema.append(field.get_field_schema())
        return table_schema

    @staticmethod
    def get_table_partitioning(table: Table) -> dict:
        partitioning = table.partitioning
        if partitioning is not None:
            partitioning = GoogleCloudLakeHouseService.convert_partitioning(partitioning)
            partitioning = partitioning.to_dict()
        return partitioning

    @staticmethod
    def get_table_resource(table: Table) -> dict:
        table = GoogleCloudLakeHouseService.convert_table(table)
        return table.as_dict()
