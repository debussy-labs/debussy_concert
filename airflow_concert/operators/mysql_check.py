import json

import pendulum
from mysql.connector import connect
from airflow.models import SkipMixin
from airflow.operators.python_operator import PythonOperator


def check_mysql_table(entity_json_str, db_conn_data_callable):
    db_conn_data = db_conn_data_callable()
    entity_dict = json.loads(entity_json_str)
    entity = entity_dict["entity"]
    source_table = entity.get("SourceTable")
    offset_type = entity.get("OffsetType")
    offset_value = entity.get("OffsetValue")
    offset_field = entity.get("OffsetField")
    source_timezone = entity.get("SourceTimezone")

    if offset_value:
        if offset_type == "TIMESTAMP":
            offset_value = "'{}'".format(
                pendulum.parse(offset_value)
                .in_timezone(source_timezone)
                .strftime("%Y-%m-%dT%H:%M:%S")
            )
        elif offset_type == "ROWVERSION":
            offset_value = f"0x{offset_value}"
        elif offset_type == "STRING":
            offset_value = f"'{offset_value}'"

        query = (
            f"SELECT COUNT(*) FROM {source_table}"
            f" WHERE {offset_field} > {offset_value}"
        )

        with connect(**db_conn_data) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()[0][0]
    else:
        result = 0
    return result


class MySQLCheckOperator(PythonOperator, SkipMixin):
    def __init__(self, entity_json_str, db_conn_data_callable, **kwargs):
        op_args = [entity_json_str, db_conn_data_callable]

        super().__init__(python_callable=check_mysql_table, op_args=op_args, **kwargs)

    def execute(self, context):
        value = super().execute(context)
        if not value:
            self.log.info("Skipping downstream tasks...")
            downstream_tasks = context['task'].get_flat_relatives(upstream=False)
            self.log.debug("Downstream task_ids %s", downstream_tasks)
            if downstream_tasks:
                self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)
