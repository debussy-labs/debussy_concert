from debussy_concert.core.entities.table import BigQueryTable


class GoogleCloudLakeHouseService:
    @staticmethod
    def get_table_schema(table: BigQueryTable):
        table_fields = table.fields
        table_schema = []
        for field in table_fields:
            table_schema.append(field.get_field_schema())
        return table_schema
