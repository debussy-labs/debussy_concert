from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from debussy_concert.motif.motif_base import MotifBase
from debussy_concert.motif.mixins.bigquery_job import BigQueryJobMixin
from debussy_concert.phrase.protocols import PMergeTableMotif
from debussy_concert.config.movement_parameters.data_ingestion import DataIngestionMovementParameters


def build_bigquery_merge_query(
    sql_template,
    main_table,
    delta_table,
    pii_columns,
    pii_table,
    delta_date_partition,
    delta_date_value,
    delta_time_partition,
    delta_time_value,
    primary_key,
    min_partition_value,
    max_partition_value,
    fields,
    partition_field=None,
):

    if pii_columns:
        except_columns = f"EXCEPT ({pii_columns})"
        extra_columns = ",".join(f"PII.{column}" for column in pii_columns.split(","))
        join_clause = f"INNER JOIN {pii_table} PII USING ({primary_key})"
    else:
        except_columns = ""
        extra_columns = ""
        join_clause = ""

    where_clause = f"""
    {delta_date_partition} = '{delta_date_value}' AND
    {delta_time_partition} = '{delta_time_value}'
    """
    if partition_field:
        merge_clause = f"""
        main.{primary_key} = delta.{primary_key} AND
        main.{partition_field} BETWEEN '{min_partition_value}' AND '{max_partition_value}'
        """
    else:
        merge_clause = f"""
        main.{primary_key} = delta.{primary_key}
        """
    update_clause = ", ".join(f"main.{field} = delta.{field}" for field in fields)
    insert_list = ", ".join(f"`{field}`" for field in fields)
    insert_values_list = ", ".join(f"delta.{field}" for field in fields)
    return sql_template.format(
        main_table=main_table,
        except_columns=except_columns,
        extra_columns=extra_columns,
        join_clause=join_clause,
        delta_table=delta_table,
        where_clause=where_clause,
        merge_clause=merge_clause,
        update_clause=update_clause,
        insert_list=insert_list,
        insert_values_list=insert_values_list,
    )


MERGE = """
    MERGE
        `{main_table}` AS main
    USING
        (
        SELECT
            * {except_columns},
            {extra_columns}
        FROM
            `{delta_table}` {join_clause}
        WHERE
            {where_clause}
        ) AS delta
    ON
        {merge_clause}
    WHEN MATCHED THEN UPDATE SET
        {update_clause}
    WHEN NOT MATCHED THEN INSERT(
        {insert_list}
    ) VALUES (
        {insert_values_list}
    )
"""


class MergeBigQueryTableMotif(MotifBase, BigQueryJobMixin, PMergeTableMotif):
    def __init__(
        self,
        config,
        movement_parameters: DataIngestionMovementParameters,
        name=None
    ) -> None:
        self.movement_parameters = movement_parameters
        super().__init__(name=name, config=config)

    def setup(
        self,
        main_table_uri: str,
        delta_table_uri: str,
    ):
        self.main_table_uri = main_table_uri
        self.delta_table_uri = delta_table_uri
        return self

    def build(self, dag, task_group):
        task_group = TaskGroup(group_id=self.name, dag=dag, parent_group=task_group)
        build_merge_query = self.build_merge_query(dag, task_group)
        query_macro = f"{{{{ task_instance.xcom_pull('{build_merge_query.task_id}') }}}}"
        execute_query = self.insert_job_operator(dag, task_group, self.query_configuration(sql_query=query_macro))
        build_merge_query >> execute_query
        return task_group

    def build_merge_query(self, dag, task_group) -> PythonOperator:
        pii_columns = ','.join([column.name for column in self.movement_parameters.pii_columns])
        primary_key = self.movement_parameters.primary_key.name
        fields_list = [field.name for field in self.movement_parameters.fields]
        delta_date_partition = "loadDate"
        delta_date_value = "{{ ds }}"
        delta_time_partition = "loadTimestamp"
        delta_time_value = "{{ ts_nodash }}"
        build_merge_query = PythonOperator(
            task_id="build_merge_query",
            python_callable=build_bigquery_merge_query,
            op_kwargs={
                "sql_template": MERGE,
                "main_table": self.main_table_uri,
                "delta_table": self.delta_table_uri,
                "pii_columns": pii_columns,
                "pii_table": "",
                "delta_date_partition": delta_date_partition,
                "delta_date_value": delta_date_value,
                "delta_time_partition": delta_time_partition,
                "delta_time_value": delta_time_value,
                "primary_key": primary_key,
                "partition_field": None,
                "min_partition_value": None,
                "max_partition_value": None,
                "fields": fields_list,
            },
            dag=dag,
            task_group=task_group
        )

        return build_merge_query
