from debussy.core.motif.motif_base import MotifBase, PClusterMotifMixin
from debussy.core.phrase.protocols import PExportDataToStorageMotif
from debussy.core.tasks import TaskSpec, result_of
from debussy.core.tasks.functions import identity
from debussy.pipeline.data_ingestion.config.movement_parameters.rdbms_data_ingestion import (
    RdbmsDataIngestionMovementParameters,
)
from debussy.pipeline.data_ingestion.config.rdbms_data_ingestion import (
    ConfigRdbmsDataIngestion,
)
from debussy.pipeline.reverse_etl.motif.bigquery_query_job import BigQueryQueryJobMotif


class ExportBigQueryQueryToGcsMotif(BigQueryQueryJobMotif):
    extraction_query_template = """
    EXPORT DATA OPTIONS(overwrite=false,format='PARQUET',uri='{uri}')
    AS {extraction_query}
    """

    def __init__(
        self,
        extraction_query,
        gcs_partition: str,
        name=None,
        gcp_conn_id="google_cloud_default",
        **op_kw_args,
    ):
        super().__init__(name, gcp_conn_id=gcp_conn_id, **op_kw_args)
        self.extraction_query = extraction_query
        self.gcs_partition = gcs_partition

    def setup(self, destination_storage_uri):
        self.destination_storage_uri = destination_storage_uri
        uri = f"{destination_storage_uri}/" f"{self.gcs_partition}/" f"*.parquet"
        self.sql_query = self.extraction_query_template.format(
            uri=uri, extraction_query=self.extraction_query
        )

        return self


class DataprocExportRdbmsTableToGcsMotif(
    MotifBase,
    PClusterMotifMixin,
    PExportDataToStorageMotif,
):
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
    internal_ip_only = False
    idle_seconds_delete_ttl = 8 * 60  # 8 minutes

    def __init__(
        self,
        movement_parameters: RdbmsDataIngestionMovementParameters,
        gcs_partition: str,
        jdbc_driver,
        jdbc_url,
        main_python_file_uri,
        name=None,
    ) -> None:
        super().__init__(name=name)
        self.gcs_partition = gcs_partition
        self.jdbc_driver = jdbc_driver
        self.jdbc_url = jdbc_url
        self.main_python_file_uri = main_python_file_uri
        self.movement_parameters = movement_parameters
        self.pip_packages = self.config.dataproc_config.get("pip_packages", [])
        self.spark_jars_packages = self.config.dataproc_config.get(
            "spark_jars_packages", ""
        )
        self.service_account_scopes = self.config.dataproc_config.get(
            "service_account_scopes", self.service_account_scopes
        )
        self.cluster_tags = self.config.dataproc_config.get(
            "cluster_tags", self.cluster_tags
        )
        self.gcs_connector_version = self.config.dataproc_config.get(
            "gcs_connector_version", self.gcs_connector_version
        )
        self.bigquery_connector_version = self.config.dataproc_config.get(
            "bigquery_connector_version", self.bigquery_connector_version
        )
        self.spark_bigquery_connector_version = self.config.dataproc_config.get(
            "spark_bigquery_connector_version", self.spark_bigquery_connector_version
        )
        self.master_machine_type_uri = self.config.dataproc_config.get(
            "master_machine_type_uri", self.master_machine_type_uri
        )
        self.software_config_image_version = self.config.dataproc_config.get(
            "software_config_image_version", self.software_config_image_version
        )
        self.endpoint_enable_http_port_access = self.config.dataproc_config.get(
            "endpoint_enable_http_port_access", self.endpoint_enable_http_port_access
        )
        self.internal_ip_only = self.config.dataproc_config.get(
            "internal_ip_only", self.internal_ip_only
        )

    @property
    def config(self) -> ConfigRdbmsDataIngestion:
        return super().config

    @property
    def cluster_name(self):
        # max number of characters for dataproc cluster names is 34
        return (
            f"dby{result_of('cluster_name_id')}"
            f"{self.config.source_name.replace('_', '').lower()[:22]}"
        )

    @property
    def cluster_config(self):
        environment = self.config.environment
        project = environment.project
        region = environment.region
        zone = environment.zone
        staging_bucket = environment.staging_bucket

        from google.protobuf.duration_pb2 import Duration

        init_action_timeout = Duration()
        init_action_timeout.FromSeconds(500)
        cluster_config = {
            "temp_bucket": staging_bucket,
            "gce_cluster_config": {
                "zone_uri": zone,
                "subnetwork_uri": self.config.dataproc_config["subnet"],
                "internal_ip_only": self.internal_ip_only,
                "tags": self.cluster_tags,
                "metadata": {
                    "gcs-connector-version": self.gcs_connector_version,
                    "bigquery-connector-version": self.bigquery_connector_version,
                    "spark-bigquery-connector-version": self.spark_bigquery_connector_version,
                    "PIP_PACKAGES": " ".join(self.pip_packages),
                },
                "service_account_scopes": self.service_account_scopes,
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
            "endpoint_config": {
                "enable_http_port_access": self.endpoint_enable_http_port_access
            },
            "lifecycle_config": {
                "idle_delete_ttl": {"seconds": self.idle_seconds_delete_ttl}
            },
        }
        return cluster_config

    def setup(self, destination_storage_uri: str):
        self.destination_storage_uri = destination_storage_uri
        return self

    def tasks(self):
        environment = self.config.environment
        secret_uri = f"{self.config.secret_manager_uri}/versions/latest"
        gcp_conn_id = environment.data_lakehouse_connection_id
        cluster = {
            "project_id": environment.project,
            "region": environment.region,
            "cluster_name": self.cluster_name,
            "gcp_conn_id": gcp_conn_id,
        }
        return [
            begin_task(self.movement_parameters.name),
            short_id_task("cluster_name_id"),
            TaskSpec(
                name="create_dataproc_cluster",
                action="dataproc.create_cluster",
                params={**cluster, "cluster_config": self.cluster_config},
                templated={"cluster_name"},
            ),
            TaskSpec(
                name="jdbc_to_raw_vault",
                action="dataproc.submit_job",
                params={
                    "job": {
                        "reference": {"project_id": environment.project},
                        "placement": {"cluster_name": self.cluster_name},
                        "pyspark_job": {
                            "main_python_file_uri": self.main_python_file_uri,
                            "args": [
                                self.jdbc_driver,
                                self.jdbc_url,
                                secret_uri,
                                self.config.source_name,
                                self.movement_parameters.extraction_query,
                                "{{ ts_nodash }}",
                                f"{self.destination_storage_uri}/{self.gcs_partition}",
                            ],
                        },
                    },
                    "region": environment.region,
                    "project_id": environment.project,
                    "gcp_conn_id": gcp_conn_id,
                },
                templated={"job"},
            ),
            TaskSpec(
                name="delete_dataproc_cluster",
                action="dataproc.delete_cluster",
                params=cluster,
                templated={"cluster_name"},
                always_run=True,
            ),
        ]


class DataprocServerlessExportRdbmsTableToGcsMotif(
    MotifBase, PExportDataToStorageMotif
):
    config: ConfigRdbmsDataIngestion

    def __init__(
        self,
        movement_parameters: RdbmsDataIngestionMovementParameters,
        gcs_partition: str,
        jdbc_driver,
        jdbc_url,
        main_python_file_uri,
        name=None,
    ) -> None:
        super().__init__(name=name)
        self.gcs_partition = gcs_partition
        self.jdbc_driver = jdbc_driver
        self.jdbc_url = jdbc_url
        self.main_python_file_uri = main_python_file_uri
        self.movement_parameters = movement_parameters
        self.pip_packages = self.config.dataproc_config.get("pip_packages", [])
        self.spark_jars_packages = self.config.dataproc_config.get(
            "spark_jars_packages", ""
        )

    @property
    def config(self) -> ConfigRdbmsDataIngestion:
        return super().config

    @property
    def batch_id(self):
        # max number of characters for dataproc serverless names is 34
        return (
            f"dby{result_of('batch_job_id')}"
            f"-{self.config.source_name.replace('_', '').lower()[:22]}"
            f"-{self.movement_parameters.name.replace('_', '').lower()[:22]}"
        )

    @property
    def batch_config(self):
        secret_uri = f"{self.config.secret_manager_uri}/versions/latest"
        run_ts = "{{ ts_nodash }}"

        batch = {
            "pyspark_batch": {
                "main_python_file_uri": self.main_python_file_uri,
                "args": [
                    self.jdbc_driver,
                    self.jdbc_url,
                    secret_uri,
                    self.config.source_name,
                    self.movement_parameters.extraction_query,
                    run_ts,
                    f"{self.destination_storage_uri}/{self.gcs_partition}",
                ],
                "jar_file_uris": self.spark_jars_packages,
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": self.config.dataproc_config["subnet"]
                },
            },
        }

        return batch

    def setup(self, destination_storage_uri: str):
        self.destination_storage_uri = destination_storage_uri
        return self

    def tasks(self):
        environment = self.config.environment
        return [
            begin_task(self.movement_parameters.name),
            short_id_task("batch_job_id"),
            TaskSpec(
                name="create_dataproc_serverless",
                action="dataproc.create_batch",
                params={
                    "project_id": environment.project,
                    "batch": self.batch_config,
                    "region": environment.region,
                    "batch_id": self.batch_id,
                    "gcp_conn_id": environment.data_lakehouse_connection_id,
                },
                templated={"batch", "batch_id"},
            ),
        ]


def begin_task(phase: str) -> TaskSpec:
    """The first task of a phase, which returns the run's dates."""
    return TaskSpec(
        name=f"begin_{phase}", action="debussy.begin", params={"phase": phase}
    )


def short_id_task(name: str) -> TaskSpec:
    """A task whose result is a short id unique to the task instance, used to
    name cloud resources."""
    return TaskSpec(
        name=name,
        action=identity,
        params={"value": "{{ ti.id.hex[:8] }}"},
        templated={"value"},
    )
