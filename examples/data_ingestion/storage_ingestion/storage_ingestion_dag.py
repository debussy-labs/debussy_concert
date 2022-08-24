import datetime as dt
from dataclasses import dataclass

from airflow.configuration import conf
from debussy_concert.core.config.config_dag_parameters import \
    ConfigDagParameters
from debussy_concert.core.config.config_environment import ConfigEnvironment
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.core.phrase.protocols import PExportDataToStorageMotif
from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.workflow.airflow import AirflowService
from debussy_concert.pipeline.data_ingestion.composition.base import \
    DataIngestionBase
from debussy_concert.pipeline.data_ingestion.config.base import \
    ConfigDataIngestionBase
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.time_partitioned import (
    BigQueryDataPartitioning, TimePartitionedDataIngestionMovementParameters)
from debussy_concert.pipeline.data_ingestion.phrase.ingestion_to_raw_vault import \
    IngestionSourceToRawVaultStoragePhrase
from debussy_framework.v3.hooks.storage_hook import GCSHook, S3Hook
from debussy_framework.v3.operators.storage_to_storage import \
    StorageToStorageOperator

dags_folder = conf.get('core', 'dags_folder')


@dataclass(frozen=True)
class StorageParquetDataIngestionMovementParameters(TimePartitionedDataIngestionMovementParameters):
    source_storage_type: str  # 'gcs' or 's3'
    source_file_uri: str


class SourceToGcsMotif(MotifBase, PExportDataToStorageMotif):
    def __init__(self, source_storage_hook,
                 lakehouse_gcp_conn_id,
                 source_file_uri,
                 gcs_partition,
                 name=None):
        super().__init__(name=name)
        self.source_storage_hook = source_storage_hook
        self.lakehouse_gcs_hook = GCSHook(gcp_conn_id=lakehouse_gcp_conn_id)
        self.source_file_uri = source_file_uri
        self.gcs_partition = gcs_partition

    def setup(self, destination_storage_uri: str):
        self.destination_storage_uri = destination_storage_uri
        self.gcs_schema_uri = (
            f'{destination_storage_uri}/'
            f'{self.gcs_partition}/'
        )

    def build(self, dag, phrase_group):
        gcs_to_gcs_task = StorageToStorageOperator(
            task_id='source_to_raw_vault_gcs',
            origin_storage_hook=self.source_storage_hook,
            origin_file_uri=self.source_file_uri,
            destination_storage_hook=self.lakehouse_gcs_hook,
            destination_file_uri=self.gcs_schema_uri + '0.parquet',
            dag=dag,
            task_group=phrase_group
        )
        return gcs_to_gcs_task


class StorageParquetDataIngestionComposition(DataIngestionBase):
    def source_to_raw_vault_gcs_motif(self, source_storage_hook,
                                      movement_parameters: StorageParquetDataIngestionMovementParameters):
        return SourceToGcsMotif(
            source_storage_hook=source_storage_hook,
            lakehouse_gcp_conn_id=self.config.environment.data_lakehouse_connection_id,
            source_file_uri=movement_parameters.source_file_uri,
            gcs_partition=movement_parameters.data_partitioning.gcs_partition_schema
        )

    def gcs_to_raw_vault_gcs_motif(self, movement_parameters: StorageParquetDataIngestionMovementParameters):
        source_storage_hook = GCSHook(gcp_conn_id=movement_parameters.extract_connection_id)
        return self.source_to_raw_vault_gcs_motif(source_storage_hook, movement_parameters)

    def s3_to_raw_vault_gcs_motif(self, movement_parameters: StorageParquetDataIngestionMovementParameters):
        source_storage_hook = S3Hook(aws_conn_id=movement_parameters.extract_connection_id)
        return self.source_to_raw_vault_gcs_motif(source_storage_hook, movement_parameters)

    def get_motif_from_source(self, movement_parameters: StorageParquetDataIngestionMovementParameters):
        source_map = {
            'gcs': self.gcs_to_raw_vault_gcs_motif,
            's3': self.s3_to_raw_vault_gcs_motif
        }
        return source_map[movement_parameters.source_storage_type](movement_parameters)

    def storage_ingestion_to_raw_vault_phrase(self, export_data_to_storage_motif):
        return IngestionSourceToRawVaultStoragePhrase(export_data_to_storage_motif=export_data_to_storage_motif)

    def gcs_data_ingestion_movement_builder(self, movement_parameters: StorageParquetDataIngestionMovementParameters):

        data_to_raw_vault_motif = self.get_motif_from_source(movement_parameters)
        movement_builder = self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.storage_ingestion_to_raw_vault_phrase(data_to_raw_vault_motif)
        )
        return movement_builder


@dataclass
class SourceInfo:
    storage_type: str
    extract_connection_id: str
    file_uri: str
    raw_table_definition: str


gcs_source = SourceInfo(
    storage_type='gcs',
    extract_connection_id='google_cloud_debussy',
    file_uri=(
        'gs://dotz-datalake-dev-l2-raw-vault/bigquery/example/sintetico_full/'
        '_load_flag=full/_logical_ts=1970-01-01/_ingestion_ts=2022-04-29 16:33:00.218627+00:00/'
        '000000000000.parquet'),
    raw_table_definition=f'{dags_folder}/examples/storage_ingestion/table_schemas/table_def_gcs.yaml'
)
s3_source = SourceInfo(
    storage_type='s3',
    extract_connection_id='aws_noverde',
    file_uri='s3://dotz-integracao-stg/0.parquet',
    raw_table_definition=f'{dags_folder}/examples/storage_ingestion/table_schemas/table_def_s3.yaml'
)

movements_parameters = []
for source in (gcs_source, s3_source):
    movement_parameters = StorageParquetDataIngestionMovementParameters(
        name=f'{source.storage_type}_parquet_ingestion_example',
        source_file_uri=source.file_uri,
        source_storage_type=source.storage_type,
        extract_connection_id=source.extract_connection_id,
        raw_table_definition=source.raw_table_definition,
        data_partitioning=BigQueryDataPartitioning(
            gcs_partition_schema='_load_flag=full/_logical_ts=1970-01-01/_ingestion_ts={{ dag_run.start_date }}',
            destination_partition=1970
        )
    )
    movements_parameters.append(movement_parameters)

dag_parameters = ConfigDagParameters(
    dag_id="storage_data_ingestion_example",
    default_args={
        'owner': 'debussy_concert'
    },
    description="Storage ingestion example",
    start_date=dt.datetime(2022, 1, 1),
    schedule_interval=None,
    tags=['project:example', 'tier:5', 'source:gcs',
          'framework:debussy_concert', 'load:full', 'type:ingestion'],

)
workflow_service = AirflowService()
env_file_path = f'{dags_folder}/examples/storage_ingestion/environment.yaml'
env_config = ConfigEnvironment.load_from_file(env_file_path)
config_composition = ConfigDataIngestionBase(
    name='storage_data_ingestion_example',
    description='Storage data ingestion example',
    movements_parameters=movements_parameters,
    environment=env_config,
    dag_parameters=dag_parameters,
    source_type='storage',
    source_name='debussy_concert_example'
)

inject_dependencies(workflow_service, config_composition)
composition = StorageParquetDataIngestionComposition()
dag = composition.build(composition.gcs_data_ingestion_movement_builder)
