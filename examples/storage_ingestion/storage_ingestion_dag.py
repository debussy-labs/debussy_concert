import datetime as dt
from dataclasses import dataclass
from airflow.configuration import conf
from debussy_concert.core.phrase.protocols import PExportDataToStorageMotif
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.core.config.config_dag_parameters import ConfigDagParameters
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.data_ingestion.composition.base import DataIngestionBase
from debussy_concert.data_ingestion.config.movement_parameters.time_partitioned import (
    TimePartitionedDataIngestionMovementParameters,
    BigQueryTimeDataPartitioning)
from debussy_concert.data_ingestion.phrase.ingestion_to_raw_vault import IngestionSourceToRawVaultStoragePhrase
from debussy_concert.data_ingestion.config.base import ConfigDataIngestionBase
from debussy_concert.core.service.injection import inject_dependencies
from debussy_framework.v3.operators.storage_to_storage import StorageToStorageOperator
from debussy_framework.v3.hooks.storage_hook import GCSHook


@dataclass(frozen=True)
class StorageParquetDataIngestionMovementParameters(TimePartitionedDataIngestionMovementParameters):
    source_file_uri: str


class GcsToGcsMotif(MotifBase, PExportDataToStorageMotif):
    def __init__(self, source_gcp_conn_id,
                 lake_gcp_conn_id,
                 source_file_uri,
                 gcs_partition,
                 name=None):
        super().__init__(name=name)
        self.source_gcs_hook = GCSHook(gcp_conn_id=source_gcp_conn_id)
        self.lake_gcs_hook = GCSHook(gcp_conn_id=lake_gcp_conn_id)
        self.source_file_uri = source_file_uri
        self.gcs_partition = gcs_partition

    def setup(self, destination_storage_uri: str):
        self.destination_storage_uri = destination_storage_uri
        self.gcs_schema_uri = (f'{destination_storage_uri}/'
                               f'{self.gcs_partition}/'
                               )

    def build(self, dag, phrase_group):
        gcs_to_gcs_task = StorageToStorageOperator(
            task_id='gcs_to_raw_vault_gcs',
            origin_storage_hook=self.source_gcs_hook,
            origin_file_uri=self.source_file_uri,
            destiny_storage_hook=self.lake_gcs_hook,
            destiny_file_uri=self.gcs_schema_uri + '0.parquet',
            dag=dag,
            task_group=phrase_group
        )
        return gcs_to_gcs_task


class StorageParquetDataIngestionComposition(DataIngestionBase):
    def gcs_to_raw_vault_gcs_motif(self, movement_parameters: StorageParquetDataIngestionMovementParameters):
        return GcsToGcsMotif(
            source_gcp_conn_id=movement_parameters.extract_connection_id,
            lake_gcp_conn_id=self.config.environment.data_lake_connection_id,
            source_file_uri=movement_parameters.source_file_uri,
            gcs_partition=movement_parameters.data_partitioning.gcs_partition_schema
        )

    def gcs_ingestion_to_raw_vault_phrase(self, export_data_to_storage_motif):
        return IngestionSourceToRawVaultStoragePhrase(export_data_to_storage_motif=export_data_to_storage_motif)

    def gcs_data_ingestion_movement_builder(self, movement_parameters: StorageParquetDataIngestionMovementParameters):
        data_to_raw_vault_motif = self.gcs_to_raw_vault_gcs_motif(movement_parameters)
        movement_builder = self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.gcs_ingestion_to_raw_vault_phrase(data_to_raw_vault_motif)
        )
        return movement_builder


movement_parameters = StorageParquetDataIngestionMovementParameters(
    name='gcs_parquet_ingestion_example',
    source_file_uri=('gs://dotz-datalake-dev-l2-raw-vault/bigquery/example/sintetico_full/'
                     '_load_flag=full/_logical_ts=1970-01-01/_ingestion_ts=2022-04-29 16:33:00.218627+00:00/'
                     '000000000000.parquet'),
    extract_connection_id='google_cloud_debussy',
    data_partitioning=BigQueryTimeDataPartitioning(
        partitioning_type='time',
        partition_granularity='YEAR',
        partition_field='_logical_ts',
        gcs_partition_schema='_load_flag=full/_logical_ts=1970-01-01/_ingestion_ts={{ dag_run.start_date }}',
        destination_partition=1970
    )
)

dag_parameters = ConfigDagParameters(
    dag_id="storage_data_ingestion_example",
    default_args={
        'owner': 'debussy_concert'
    },
    description="Storage ingestion example",
    start_date=dt.datetime(2022, 1, 1),
    schedule_interval=None,
    tags=['project:example', 'tier:5', 'source:gcs',
          'framework:concert', 'load:full', 'type:ingestion'],

)
workflow_service = AirflowService()
dags_folder = conf.get('core', 'dags_folder')
env_file_path = f'{dags_folder}/examples/bigquery_ingestion_inc/environment.yaml'
env_config = ConfigEnvironment.load_from_file(env_file_path)
config_composition = ConfigDataIngestionBase(
    name='storage_data_ingestion_example',
    description='Storage data ingestion example',
    movements_parameters=[movement_parameters],
    environment=env_config,
    dag_parameters=dag_parameters,
    source_type='gcs',
    source_name='debussy_concert_example'
)

inject_dependencies(workflow_service, config_composition)
composition = StorageParquetDataIngestionComposition()
dag = composition.build(composition.gcs_data_ingestion_movement_builder)
