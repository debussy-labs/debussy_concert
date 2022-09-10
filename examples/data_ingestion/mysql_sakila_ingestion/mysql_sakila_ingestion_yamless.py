import datetime as dt

from debussy_concert.core.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.pipeline.data_ingestion.composition.rdbms_ingestion import (
    RdbmsIngestionComposition,
)
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.rdbms_data_ingestion import (
    RdbmsDataIngestionMovementParameters,
)
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.time_partitioned import (
    BigQueryDataPartitioning,
)
from debussy_concert.pipeline.data_ingestion.config.rdbms_data_ingestion import (
    ConfigRdbmsDataIngestion,
)

config_environment = ConfigEnvironment(
    project="modular-aileron-191222",
    region="us-central1",
    zone="us-central1-a",
    artifact_bucket="dotz-datalake-dev-artifacts",
    staging_bucket="dotz-datalake-dev-l0-staging",
    landing_bucket="dotz-datalake-dev-l1-landing",
    raw_vault_bucket="dotz-datalake-dev-l2-raw-vault",
    reverse_etl_bucket="autodelete_dev_bucket",
    raw_vault_dataset="raw_vault",
    raw_dataset="raw",
    trusted_dataset="trusted",
    reverse_etl_dataset="reverse_etl",
    temp_dataset="temp",
    data_lakehouse_connection_id="google_cloud_debussy",
)

config_dag_parameters = ConfigDagParameters(
    dag_id="mysql_sakila_ingestion_yamless",
    default_args={"owner": "debussy"},
    description="mysql yamless ingestion example from sakila sample relational database",
    start_date=dt.datetime(2006, 2, 15),
    end_date=dt.datetime(2006, 2, 15),
    catchup=True,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=[
        "framework:debussy_concert",
        "project:example",
        "tier:5",
        "source:mysql",
        "type:ingestion",
        "load:incremental",
    ],
)

config_composition = ConfigRdbmsDataIngestion(
    name="mysql_sakila_ingestion_yamless",
    description="mysql ingestion yamless",
    source_type="mysql",
    source_name="sakila",
    secret_manager_uri=f"projects/{config_environment.project}/secrets/debussy_mysql_dev",
    dataproc_config={
        "machine_type": "n1-standard-2",
        "num_workers": 0,
        "subnet": "subnet-cluster-services",
        "parallelism": 60,
        "pip_packages": ["google-cloud-secret-manager"],
    },
    environment=config_environment,
    dag_parameters=config_dag_parameters,
    movements_parameters=[
        RdbmsDataIngestionMovementParameters(
            name="category",
            extract_connection_id="google_cloud_debussy",
            data_partitioning=BigQueryDataPartitioning(
                gcs_partition_schema=(
                    "_load_flag=incr/_ts_window_start={{ execution_date.strftime('%Y-%m-%d 00:00:00') "
                    "}}/_ts_window_end={{ next_execution_date.strftime('%Y-%m-%d 00:00:00') "
                    "}}/_ts_logical={{ execution_date.strftime('%Y-%m-%d %H:%M:%S%z') "
                    "}}/_ts_ingestion={{ dag_run.start_date.strftime('%Y-%m-%d %H:%M:%S%z') }}"
                ),
                destination_partition="{{ execution_date.strftime('%Y%m%d') }}",
            ),
            raw_table_definition={
                "fields": [
                    {
                        "name": "category_id",
                        "data_type": "INT64",
                        "description": "A surrogate primary key used to uniquely identify each category in the table",
                    },
                    {
                        "name": "name",
                        "data_type": "STRING",
                        "description": "The name of the category",
                    },
                    {
                        "name": "last_update",
                        "data_type": "TIMESTAMP",
                        "description": "When the row was created or most recently updated",
                    },
                    {
                        "name": "_load_flag",
                        "data_type": "STRING",
                        "description": "incr = incremental data ingestion; full = full data ingestion",
                    },
                    {
                        "name": "_ts_window_start",
                        "data_type": "TIMESTAMP",
                        "description": "Ingestion window start at source timezone",
                    },
                    {
                        "name": "_ts_window_end",
                        "data_type": "TIMESTAMP",
                        "description": "Ingestion window end at source timezone",
                    },
                    {
                        "name": "_ts_logical",
                        "data_type": "TIMESTAMP",
                        "description": "Airflow logical date",
                    },
                    {
                        "name": "_ts_ingestion",
                        "data_type": "TIMESTAMP",
                        "description": "Clock time at Airflow when the ingestion was executed",
                    },
                    {
                        "name": "_hash_key",
                        "data_type": "STRING",
                        "description": "An MD5 surrogate hash key used to uniquely identify each record of the source",
                    },
                ],
                "partitioning": {
                    "field": "_ts_window_start",
                    "type": "time",
                    "granularity": "DAY",
                },
            },
            extraction_query=(
                "SELECT category_id, name, last_update "
                "FROM sakila.category "
                "WHERE last_update >= '{{ execution_date.strftime('%Y-%m-%d 00:00:00') }}' "
                "AND last_update   <  '{{ next_execution_date.strftime('%Y-%m-%d 00:00:00') }}' "
            ),
        )
    ],
)

workflow_service = AirflowService()

inject_dependencies(workflow_service, config_composition)

debussy_composition = RdbmsIngestionComposition()
debussy_composition.dataproc_main_python_file_uri = (
    f"gs://{config_composition.environment.artifact_bucket}/pyspark-scripts"
    "/jdbc-to-gcs/jdbc_to_gcs_hash_key.py"
)

dag = debussy_composition.auto_play()
