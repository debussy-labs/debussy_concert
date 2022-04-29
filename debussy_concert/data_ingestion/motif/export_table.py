from typing import List
from google.protobuf.duration_pb2 import Duration
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from debussy_framework.v3.operators.mysql_check import MySQLCheckOperator
from debussy_framework.v2.operators.basic import StartOperator
from debussy_framework.v2.operators.datastore import DatastoreGetEntityOperator

from debussy_concert.core.motif.motif_base import MotifBase, PClusterMotifMixin
from debussy_concert.core.motif.mixins.dataproc import DataprocClusterHandlerMixin
from debussy_concert.core.motif.bigquery_query_job import BigQueryQueryJobMotif
from debussy_concert.core.phrase.protocols import PExportDataToStorageMotif
from debussy_concert.data_ingestion.config.movement_parameters.rdbms_data_ingestion import RdbmsDataIngestionMovementParameters
from debussy_concert.data_ingestion.config.rdbms_data_ingestion import ConfigRdbmsDataIngestion


class ExportBigQueryQueryToGcsMotif(BigQueryQueryJobMotif):
    extract_query_template = """
    EXPORT DATA OPTIONS(overwrite=false,format='PARQUET',uri='{uri}')
    AS {extract_query}
    """

    def __init__(self, extract_query, gcs_partition: str,
                 name=None, gcp_conn_id='google_cloud_default', **op_kw_args):
        super().__init__(name, gcp_conn_id=gcp_conn_id, **op_kw_args)
        self.extract_query = extract_query
        self.gcs_partition = gcs_partition

    def setup(self, destination_storage_uri):
        self.destination_storage_uri = destination_storage_uri
        uri = (f'{destination_storage_uri}/'
               f'{self.gcs_partition}/'
               f'*.parquet')
        self.sql_query = self.extract_query_template.format(
            uri=uri, extract_query=self.extract_query)

        return self


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


class ExportFullMySqlTableToGcsMotif(
        MotifBase, DataprocClusterHandlerMixin, PClusterMotifMixin, PExportDataToStorageMotif):
    config: ConfigRdbmsDataIngestion

    def __init__(
            self,
            movement_parameters: RdbmsDataIngestionMovementParameters,
            name=None
    ) -> None:
        self.movement_parameters = movement_parameters
        super().__init__(name=name)

    @property
    def config(self) -> ConfigRdbmsDataIngestion:
        return super().config

    @property
    def cluster_name(self):
        return 'pixdict-motif-cluster'

    @property
    def cluster_config(self):
        environment = self.config.environment
        project = environment.project
        region = environment.region
        zone = environment.zone
        staging_bucket = environment.staging_bucket

        init_action_timeout = Duration()
        init_action_timeout.FromSeconds(500)
        cluster_config = {
            "temp_bucket": staging_bucket,
            "gce_cluster_config": {
                "zone_uri": zone,
                "subnetwork_uri": self.config.dataproc_config["subnet"],
                "tags": ["dataproc"],
                "metadata": {
                    "gcs-connector-version": "2.2.0",
                    "bigquery-connector-version": "1.2.0",
                    "spark-bigquery-connector-version": "0.19.1",
                    "PIP_PACKAGES": "pydeequ google-cloud-secret-manager",
                },
                "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"]
            },
            "master_config": {"machine_type_uri": "n1-standard-4"},
            "software_config": {
                "image_version": "1.4",
                "properties": {
                    "spark:spark.default.parallelism": str(
                        self.config.dataproc_config["parallelism"]
                    ),
                    "spark:spark.sql.shuffle.partitions": str(
                        self.config.dataproc_config["parallelism"]
                    ),
                    "spark:spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
                    "spark:spark.jars.packages": ("com.amazon.deequ:deequ:1.1.0_spark-2.4-scala-2.11,"
                                                  "com.microsoft.sqlserver:mssql-jdbc:9.2.1.jre8"),
                    "spark:spark.jars.excludes": "net.sourceforge.f2j:arpack_combined_all",
                    "dataproc:dataproc.conscrypt.provider.enable": "false",
                },
            },
            "worker_config": {
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 1000,
                },
                "machine_type_uri": self.config.dataproc_config["machine_type"],
                "num_instances": 2,
            },
            "secondary_worker_config": {
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 1000,
                },
                "machine_type_uri": self.config.dataproc_config["machine_type"],
                "num_instances": self.config.dataproc_config["num_workers"],
            },
            "autoscaling_config": {
                "policy_uri": f"projects/{project}/regions/{region}/autoscalingPolicies/ephemeral-clusters"
            },
            "initialization_actions": [
                {
                    "executable_file": f"gs://goog-dataproc-initialization-actions-{region}/python/pip-install.sh",
                    "execution_timeout": init_action_timeout,
                },
                {
                    "executable_file": f"gs://goog-dataproc-initialization-actions-{region}/connectors/connectors.sh",
                    "execution_timeout": init_action_timeout,
                },
            ],
            "endpoint_config": {"enable_http_port_access": True},
        }
        return cluster_config

    def setup(self, destination_storage_uri: str):
        self.destination_storage_uri = destination_storage_uri
        return self

    def get_db_conn_data(self):
        """Get database connection data from Secret Manager"""
        from google.cloud import secretmanager
        import json

        client = secretmanager.SecretManagerServiceClient()

        name = f"{self.config.secret_manager_uri}/versions/latest"
        response = client.access_secret_version(name=name)
        secret = response.payload.data.decode("UTF-8")
        db_conn_data = json.loads(secret)
        db_conn_data.update({"database": self.config.source_name})
        return db_conn_data

    def build(self, dag, parent_task_group: TaskGroup):
        task_group = TaskGroup(group_id=self.name, parent_group=parent_task_group)

        start = StartOperator(phase=self.movement_parameters.name, dag=dag, task_group=task_group)

        get_datastore_entity = self.get_datastore_entity(dag, self.movement_parameters, task_group)
        check_mysql_table = self.check_mysql_table(dag, task_group, get_datastore_entity.task_id)
        build_extract_query = self.build_extract_query(dag, task_group, get_datastore_entity.task_id)
        create_dataproc_cluster = self.create_dataproc_cluster(dag, task_group)
        jdbc_to_raw_vault = self.jdbc_to_raw_vault(dag, task_group, build_extract_query.task_id)
        delete_dataproc_cluster = self.delete_dataproc_cluster(dag, task_group)
        (
            start >>
            get_datastore_entity >>
            check_mysql_table >>
            build_extract_query >>
            create_dataproc_cluster >>
            jdbc_to_raw_vault >>
            delete_dataproc_cluster
        )
        return task_group

    def jdbc_to_raw_vault(self, dag, task_group, build_extract_query_id):
        secret_uri = f"{self.config.secret_manager_uri}/versions/latest"
        run_ts = "{{ ts_nodash }}"

        # path and naming parameters
        load_timestamp_partition = "loadTimestamp"
        run_ts = "{{ ts_nodash }}"
        load_date_partition = "loadDate"
        run_date = "{{ ds }}"
        pyspark_scripts_uri = f"gs://{self.config.environment.artifact_bucket}/pyspark-scripts"

        driver = "com.mysql.cj.jdbc.Driver"
        jdbc_url = "jdbc:mysql://{host}:{port}/" + self.config.source_name

        jdbc_to_raw_vault = DataprocSubmitJobOperator(
            task_id="jdbc_to_raw_vault",
            job={
                    "reference": {"project_id": self.config.environment.project},
                    "placement": {"cluster_name": self.cluster_name},
                    "pyspark_job": {
                        "main_python_file_uri": f"{pyspark_scripts_uri}/jdbc-to-gcs/jdbc_to_gcs.py",
                        "args": [
                            driver,
                            jdbc_url,
                            secret_uri,
                            self.config.source_name,
                            f"{{{{ task_instance.xcom_pull('{build_extract_query_id}') }}}}",
                            run_ts,
                            (f"{self.destination_storage_uri}/{load_date_partition}={run_date}/"
                             f"{load_timestamp_partition}={run_ts}/"),
                        ],
                    },
            },
            region=self.config.environment.region,
            project_id=self.config.environment.project,
            dag=dag,
            task_group=task_group
        )

        return jdbc_to_raw_vault

    def build_extract_query(self, dag, task_group, get_datastore_entity_task_id):
        build_extract_query = PythonOperator(
            task_id="build_extract_query",
            python_callable=build_query_from_datastore_entity_json,
            op_args=[
                    f"{{{{ task_instance.xcom_pull('{get_datastore_entity_task_id}') }}}}"],
            dag=dag,
            task_group=task_group
        )

        return build_extract_query

    def check_mysql_table(self, dag, task_group, get_datastore_entity_task_id):
        check_mysql_table = MySQLCheckOperator(
            task_id="check_mysql_table",
            entity_json_str=f"{{{{ task_instance.xcom_pull('{get_datastore_entity_task_id}') }}}}",
            db_conn_data_callable=self.get_db_conn_data,
            dag=dag,
            task_group=task_group
        )

        return check_mysql_table

    def get_datastore_entity(self, dag, movement_parameters: RdbmsDataIngestionMovementParameters, task_group):
        db_kind = self.config.source_name[0].upper() + self.config.source_name[1:]
        kind = f"MySql{db_kind}Tables"
        get_datastore_entity = DatastoreGetEntityOperator(
            task_id="get_datastore_entity",
            project=self.config.environment.project,
            namespace="TABLE",
            kind=kind,
            filters=("SourceTable", "=", movement_parameters.name),
            dag=dag,
            task_group=task_group
        )

        return get_datastore_entity
