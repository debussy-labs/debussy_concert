from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.operators.basic import StartOperator
from airflow_concert.operators.datastore import DatastoreGetEntityOperator
from airflow_concert.operators.mysql_check import MySQLCheckOperator
from airflow_concert.entities.table import Table


class ExportBigQueryTablePhrase(PhraseBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name, config=config)

    def build(self, dag, task_group):
        operator = DummyOperator(task_id=self.name, dag=dag, task_group=task_group)
        return operator


def build_query_from_datastore_entity_json(entity_json_str):
    import json
    import pendulum

    entity_dict = json.loads(entity_json_str)
    entity = entity_dict["entity"]
    source_table = entity.get("SourceTable")
    fields = entity.get("Fields")
    fields = fields.split(",")
    if "METADATA" in fields:
        fields.remove("METADATA")

    fields = [f"`{field}`" for field in fields]
    fields = ", ".join(fields)
    offset_type = entity.get("OffsetType")
    offset_value = entity.get("OffsetValue")
    offset_field = entity.get("OffsetField")
    source_timezone = entity.get("SourceTimezone")
    if offset_value == "NONE":
        offset_value = None
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

    if offset_value:
        query = (
            f"SELECT {fields} FROM {source_table}"
            f" WHERE {offset_field} > {offset_value}"
        )
    else:
        query = f"SELECT {fields} FROM {source_table}"

    return query


class ExportMySqlTablePhrase(PhraseBase):
    def __init__(self, config, table: Table, name=None) -> None:
        self.table = table
        super().__init__(name=name, config=config)

    def get_db_conn_data(self):
        """Get database connection data from Secret Manager"""
        from google.cloud import secretmanager
        import json

        client = secretmanager.SecretManagerServiceClient()

        name = f"projects/{self.project_id}/secrets/{self.config.secret_id}/versions/latest"
        response = client.access_secret_version(name=name)
        secret = response.payload.data.decode("UTF-8")
        db_conn_data = json.loads(secret)
        db_conn_data.update({"database": self.config.database})
        return db_conn_data

    def build(self, dag, parent_task_group: TaskGroup):
        table = self.table
        task_group = TaskGroup(group_id=self.name, parent_group=parent_task_group)
        db_kind = table.name[0].upper() + table.name[1:]
        kind = f"GCS{db_kind}Tables"
        start = StartOperator(phase=table.name, dag=dag, task_group=task_group)
        get_datastore_entity = DatastoreGetEntityOperator(
            task_id="get_datastore_entity",
            project=self.config.environment.project,
            namespace="TABLE",
            kind=kind,
            filters=("SourceTable", "=", table.name)
        )
        check_mysql_table = MySQLCheckOperator(
            task_id="check_mysql_table",
            entity_json_str="{{ task_instance.xcom_pull('get_datastore_entity') }}",
            db_conn_data_callable=self.get_db_conn_data,
        )
        build_extract_query = PythonOperator(
            task_id="build_extract_query",
            python_callable=build_query_from_datastore_entity_json,
            op_args=[
                    "{{ task_instance.xcom_pull('get_datastore_entity') }}"]
        )
        start >> get_datastore_entity >> check_mysql_table >> build_extract_query
        return task_group
