from debussy_concert.core.entities.table import BigQueryTable


class GoogleCloudLakeHouseService:
    @staticmethod
    def get_table_schema(table: BigQueryTable):
        table_fields = table.schema.fields
        table_schema = []
        for field in table_fields:
            table_schema.append(field.get_field_schema())
        return table_schema

    @staticmethod
    def get_table_partitioning(table: BigQueryTable):
        partitioning = table.partitioning
        if partitioning is not None:
            partitioning = partitioning.to_dict()
        return partitioning

    @staticmethod
    def get_table_resource(table: BigQueryTable):
        print(table.as_dict())
        return table.as_dict()
