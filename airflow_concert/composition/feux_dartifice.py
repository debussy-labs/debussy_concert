from typing import Callable

from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.entities.table import Table

from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.movement.data_ingestion import DataIngestionMovement
from airflow_concert.movement.movement_base import PMovement

from airflow_concert.phrase.ingestion_to_landing import IngestionSourceToLandingStoragePhrase
from airflow_concert.phrase.landing_to_raw import LandingStorageExternalTableToDataWarehouseRawPhrase
from airflow_concert.phrase.raw_to_trusted import DataWarehouseRawToTrustedPhrase
from airflow_concert.phrase.utils.start import StartPhrase
from airflow_concert.phrase.utils.end import EndPhrase

from airflow_concert.motif.export_table import ExportMySqlTableToGcsMotif
from airflow_concert.motif.bigquery_job import BigQueryJobMotif
from airflow_concert.motif.create_external_table import CreateExternalBigQueryTableMotif
from airflow_concert.motif.merge_table import MergeBigQueryTableMotif


class FeuxDArtifice(CompositionBase):
    def __init__(self, config: ConfigIntegration):
        super().__init__(config)

    def mysql_movement_builder(self, table: Table) -> DataIngestionMovement:
        export_mysql_to_gcs_motif = ExportMySqlTableToGcsMotif(
            config=self.config, table=table)
        ingestion_to_landing_phrase = IngestionSourceToLandingStoragePhrase(
            export_data_to_storage_motif=export_mysql_to_gcs_motif
        )
        return self.rdbms_ingestion_movement_builder(ingestion_to_landing_phrase, table)

    def rdbms_builder_fn(self) -> Callable[[Table], PMovement]:
        map_ = {
            'mysql': self.mysql_movement_builder
        }
        rdbms_name = self.config.rdbms_name
        builder = map_.get(rdbms_name)
        if not builder:
            raise RuntimeError(f"Invalid rdbms: {rdbms_name}")
        return builder

    def rdbms_ingestion_movement_builder(self, ingestion_to_landing_phrase, table: Table) -> DataIngestionMovement:
        start_phrase = StartPhrase(config=self.config)
        gcs_landing_to_bigquery_raw_phrase = self.gcs_landing_to_bigquery_raw_phrase(table)
        data_warehouse_raw_to_trusted_phrase = self.data_warehouse_raw_to_trusted_phrase()
        end_phrase = EndPhrase(config=self.config)

        name = f'Movement_{table.name}'
        ingestion = DataIngestionMovement(
            name=name,
            start_phrase=start_phrase,
            ingestion_source_to_landing_storage_phrase=ingestion_to_landing_phrase,
            landing_storage_to_data_warehouse_raw_phrase=gcs_landing_to_bigquery_raw_phrase,
            data_warehouse_raw_to_trusted_phrase=data_warehouse_raw_to_trusted_phrase,
            end_phrase=end_phrase
        )
        ingestion.setup(self.config, table)
        return ingestion

    def data_warehouse_raw_to_trusted_phrase(self) -> DataWarehouseRawToTrustedPhrase:
        bigquery_to_bigquery_motif = BigQueryJobMotif(sql_query='select 1')
        data_warehouse_raw_to_trusted_phrase = DataWarehouseRawToTrustedPhrase(
            name='Raw_to_Trusted_Phrase',
            raw_to_trusted_motif=bigquery_to_bigquery_motif
        )
        return data_warehouse_raw_to_trusted_phrase

    def gcs_landing_to_bigquery_raw_phrase(self, table: Table) -> LandingStorageExternalTableToDataWarehouseRawPhrase:
        create_external_bigquery_table_motif = self.create_external_bigquery_table_motif(table)
        merge_bigquery_table_motif = self.merge_bigquery_table_motif(table)
        gcs_landing_to_bigquery_raw_phrase = LandingStorageExternalTableToDataWarehouseRawPhrase(
            name='Landing_to_Raw_Phrase',
            create_external_table_motif=create_external_bigquery_table_motif,
            merge_table_motif=merge_bigquery_table_motif
        )
        return gcs_landing_to_bigquery_raw_phrase

    def execute_query_motif(self, sql_query):
        execute_query_motif = BigQueryJobMotif(sql_query=sql_query)
        return execute_query_motif

    def merge_bigquery_table_motif(self, table: Table) -> MergeBigQueryTableMotif:
        merge_bigquery_table_motif = MergeBigQueryTableMotif(
            config=self.config,
            table=table
        )
        return merge_bigquery_table_motif

    def create_external_bigquery_table_motif(self, table: Table) -> CreateExternalBigQueryTableMotif:
        create_external_bigquery_table_motif = CreateExternalBigQueryTableMotif(
            config=self.config,
            table=table
        )
        return create_external_bigquery_table_motif
