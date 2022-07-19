from airflow import DAG
from airflow.utils.task_group import TaskGroup
from google.protobuf.duration_pb2 import Duration
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from debussy_concert.core.phrase.protocols import PExportDataToStorageMotif
from debussy_concert.core.motif.motif_base import MotifBase, PClusterMotifMixin
from debussy_concert.core.motif.mixins.dataproc import DataprocClusterHandlerMixin
from debussy_concert.core.motif.bigquery_query_job import BigQueryQueryJobMotif
from debussy_concert.data_ingestion.config.rdbms_data_ingestion import ConfigRdbmsDataIngestion
from debussy_concert.data_ingestion.config.movement_parameters.rdbms_data_ingestion import RdbmsDataIngestionMovementParameters

from debussy_framework.v3.operators.basic import StartOperator


class ExportBigQueryQueryToGcsMotif(BigQueryQueryJobMotif):
    extraction_query_template = """
    EXPORT DATA OPTIONS(overwrite=false,format='PARQUET',uri='{uri}')
    AS {extraction_query}
    """

    def __init__(self, extraction_query, gcs_partition: str,
                 name=None, gcp_conn_id='google_cloud_default', **op_kw_args):
        super().__init__(name, gcp_conn_id=gcp_conn_id, **op_kw_args)
        self.extraction_query = extraction_query
        self.gcs_partition = gcs_partition

    def setup(self, destination_storage_uri):
        self.destination_storage_uri = destination_storage_uri
        uri = (f'{destination_storage_uri}/'
               f'{self.gcs_partition}/'
               f'*.parquet')
        self.sql_query = self.extraction_query_template.format(
            uri=uri, extraction_query=self.extraction_query)

        return self


class DataprocExportRdbmsTableToGcsMotif(
        MotifBase, DataprocClusterHandlerMixin, PClusterMotifMixin, PExportDataToStorageMotif):
    config: ConfigRdbmsDataIngestion
    cluster_tags = ["dataproc"]
    gcs_connector_version = "2.2.0"
    bigquery_connector_version = "1.2.0"
    spark_bigquery_connector_version = "0.19.1"
    service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    master_machine_type_uri = "n1-standard-4"
    software_config_image_version = "1.4"
    work_num_instances = 2
    worker_disk_config = {
        "boot_disk_type": "pd-standard",
        "boot_disk_size_gb": 1000,
    }
    endpoint_enable_http_port_access = True
    idle_seconds_delete_ttl = 300
    _cluster_name_task_id = None

    def __init__(
            self,
            movement_parameters: RdbmsDataIngestionMovementParameters,
            gcs_partition: str,
            jdbc_driver,
            jdbc_url,
            main_python_file_uri,
            name=None
    ) -> None:
        super().__init__(name=name)
        self.gcs_partition = gcs_partition
        self.jdbc_driver = jdbc_driver
        self.jdbc_url = jdbc_url
        self.main_python_file_uri = main_python_file_uri
        self.movement_parameters = movement_parameters
        self.pip_packages = self.config.dataproc_config.get(
            "pip_packages", [])
        self.spark_jars_packages = self.config.dataproc_config.get(
            "spark_jars_packages", "")
        self.service_account_scopes = self.config.dataproc_config.get(
            "service_account_scopes", self.service_account_scopes)
        self.cluster_tags = self.config.dataproc_config.get(
            "cluster_tags", self.cluster_tags)
        self.gcs_connector_version = self.config.dataproc_config.get(
            "gcs_connector_version", self.gcs_connector_version)
        self.bigquery_connector_version = self.config.dataproc_config.get(
            "bigquery_connector_version", self.bigquery_connector_version)
        self.spark_bigquery_connector_version = self.config.dataproc_config.get(
            "spark_bigquery_connector_version", self.spark_bigquery_connector_version)
        self.master_machine_type_uri = self.config.dataproc_config.get(
            "master_machine_type_uri", self.master_machine_type_uri)
        self.software_config_image_version = self.config.dataproc_config.get(
            "software_config_image_version", self.software_config_image_version)
        self.endpoint_enable_http_port_access = self.config.dataproc_config.get(
            "endpoint_enable_http_port_access", self.endpoint_enable_http_port_access)

    @property
    def config(self) -> ConfigRdbmsDataIngestion:
        return super().config

    @property
    def cluster_name(self):
        if not self._cluster_name_task_id:
            raise RuntimeError("Cluster name is not defined or being accessed before being defined")
        return self._cluster_name_task_id

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
                "tags": self.cluster_tags,
                "metadata": {
                    "gcs-connector-version": self.gcs_connector_version,
                    "bigquery-connector-version": self.bigquery_connector_version,
                    "spark-bigquery-connector-version": self.spark_bigquery_connector_version,
                    "PIP_PACKAGES": " ".join(self.pip_packages),
                },
                "service_account_scopes": self.service_account_scopes
            },
            "master_config": {"machine_type_uri": self.master_machine_type_uri},
            "software_config": {
                "image_version": self.software_config_image_version,
                "properties": {
                    "spark:spark.default.parallelism": str(
                        self.config.dataproc_config["parallelism"]
                    ),
                    "spark:spark.sql.shuffle.partitions": str(
                        self.config.dataproc_config["parallelism"]
                    ),
                    "spark:spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
                    "spark:spark.jars.packages": self.spark_jars_packages,
                    "spark:spark.jars.excludes": "net.sourceforge.f2j:arpack_combined_all",
                    "dataproc:dataproc.conscrypt.provider.enable": "false",
                },
            },
            "worker_config": {
                "disk_config": self.worker_disk_config,
                "machine_type_uri": self.config.dataproc_config["machine_type"],
                "num_instances": self.work_num_instances,
            },
            "secondary_worker_config": {
                "disk_config": self.worker_disk_config,
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
            "endpoint_config": {"enable_http_port_access": self.endpoint_enable_http_port_access},
            "lifecycle_config": {"idle_delete_ttl": {"seconds": self.idle_seconds_delete_ttl}}
        }
        return cluster_config

    def setup(self, destination_storage_uri: str):
        self.destination_storage_uri = destination_storage_uri
        return self

    def build(self, dag, parent_task_group: TaskGroup):
        task_group = TaskGroup(group_id=self.name, dag=dag, parent_group=parent_task_group)

        start = StartOperator(phase=self.movement_parameters.name, dag=dag, task_group=task_group)

        cluster_name_id = self.cluster_name_id(dag, task_group)
        self._cluster_name_task_id = self.build_cluster_name(dag, cluster_name_id)

        create_dataproc_cluster = self.create_dataproc_cluster(dag, task_group)
        jdbc_to_raw_vault = self.jdbc_to_raw_vault(dag, task_group, self.movement_parameters.extraction_query)
        delete_dataproc_cluster = self.delete_dataproc_cluster(dag, task_group)
        self.workflow_service.chain_tasks(
            start,
            cluster_name_id,
            create_dataproc_cluster,
            jdbc_to_raw_vault,
            delete_dataproc_cluster
        )
        return task_group

    def build_cluster_name(self, dag: DAG, cluster_name_task):
        # max number of characters for dataproc cluster names is 34
        # for usage in cluster_name property
        return (f"dby{{{{ ti.xcom_pull(dag_id='{dag.dag_id}', task_ids='{cluster_name_task.task_id}') }}}}"
                f"{self.config.source_name.replace('_', '').lower()[:22]}")

    def cluster_name_id(self, dag, task_group):
        cluster_name_id = PythonOperator(
            task_id='cluster_name_id',
            python_callable=lambda x: x,
            op_args=['{{ ti.job_id }}'],
            dag=dag,
            task_group=task_group)
        return cluster_name_id

    def jdbc_to_raw_vault(self, dag, task_group, extraction_query):

        secret_uri = f"{self.config.secret_manager_uri}/versions/latest"
        run_ts = "{{ ts_nodash }}"

        jdbc_to_raw_vault = DataprocSubmitJobOperator(
            task_id="jdbc_to_raw_vault",
            job={
                    "reference": {"project_id": self.config.environment.project},
                    "placement": {"cluster_name": self.cluster_name},
                    "pyspark_job": {
                        "main_python_file_uri": self.main_python_file_uri,
                        "args": [
                            self.jdbc_driver,
                            self.jdbc_url,
                            secret_uri,
                            self.config.source_name,
                            extraction_query,
                            run_ts,
                            f"{self.destination_storage_uri}/{self.gcs_partition}"
                        ],
                    },
            },
            region=self.config.environment.region,
            project_id=self.config.environment.project,
            dag=dag,
            task_group=task_group
        )

        return jdbc_to_raw_vault
