from typing import Callable
from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.movement.data_ingestion import DataIngestionMovement
from airflow_concert.phrase.ingestion_to_landing import IngestionSourceToLandingStoragePhrase
from airflow_concert.phrase.landing_to_raw import LandingStorageToDataWarehouseRawPhrase
from airflow_concert.phrase.raw_to_trusted import DataWarehouseRawToTrustedPhrase
from airflow_concert.phrase.utils.start import StartPhrase
from airflow_concert.phrase.utils.end import EndPhrase
from airflow_concert.motif.export_table import ExportMySqlTableToGcsMotif
from airflow_concert.motif.bigquery_to_bigquery import BigQueryToBigQueryMotif
from airflow_concert.motif.create_external_table import CreateExternalBigQueryTableMotif
from airflow_concert.motif.merge_table import MergeReplaceBigQueryMotif
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.entities.table import Table


class Debussy(CompositionBase):
    def __init__(self, config: ConfigIntegration):
        super().__init__(config)

    def landing_bucket_uri_prefix(self, rdbms: str, table: Table):
        return (f"gs://{self.config.environment.landing_bucket}/"
                f"{rdbms}/{self.config.database}/{table.name}")

    def mysql_movement(self, table: Table) -> None:
        rdbms = 'mysql'
        export_mysql_to_gcs_motif = ExportMySqlTableToGcsMotif(
            config=self.config, table=table,
            destination_bucket_uri=self.landing_bucket_uri_prefix(rdbms=rdbms, table=table))
        ingestion_to_landing_phrase = IngestionSourceToLandingStoragePhrase(
            export_data_to_storage_motif=export_mysql_to_gcs_motif
        )
        return self.rdbms_ingestion_movement(ingestion_to_landing_phrase, table, rdbms=rdbms)

    def rdbms_ingestion_movement(self, ingestion_to_landing_phrase, table: Table, rdbms: str):

        start_phrase = StartPhrase(config=self.config)
        gcs_landing_to_bigquery_raw_phrase = self.gcs_landing_to_bigquery_raw_phrase(table, rdbms)
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
        return ingestion

    def data_warehouse_raw_to_trusted_phrase(self):
        raw_to_trusted_motif = BigQueryToBigQueryMotif(self.config)
        data_warehouse_raw_to_trusted_phrase = DataWarehouseRawToTrustedPhrase(
            name='Raw_to_Trusted_Phrase',
            raw_to_trusted_motif=raw_to_trusted_motif
        )

        return data_warehouse_raw_to_trusted_phrase

    def gcs_landing_to_bigquery_raw_phrase(self, table, rdbms):
        table_prefix = self.config.database.lower()
        create_external_bigquery_table_motif = self.create_external_bigquery_table_motif(table, rdbms, table_prefix)
        merge_replace_bigquery_motif = self.merge_replace_bigquery_motif(table, table_prefix)
        gcs_landing_to_bigquery_raw_phrase = LandingStorageToDataWarehouseRawPhrase(
            name='Landing_to_Raw_Phrase',
            create_external_table_motif=create_external_bigquery_table_motif,
            merge_landing_to_raw_motif=merge_replace_bigquery_motif
        )

        return gcs_landing_to_bigquery_raw_phrase

    def merge_replace_bigquery_motif(self, table: Table, table_prefix):
        main_table = (f"{self.config.environment.project}."
                      f"{self.config.environment.raw_dataset}."
                      f"{table_prefix}_{table.name}")
        delta_table = (f"{self.config.environment.project}."
                       f"{self.config.environment.landing_dataset}."
                       f"{table_prefix}_{table.name}")
        merge_replace_bigquery_motif = MergeReplaceBigQueryMotif(
            config=self.config,
            table=table,
            main_table_uri=main_table,
            delta_table_uri=delta_table
        )

        return merge_replace_bigquery_motif

    def create_external_bigquery_table_motif(self, table: Table, rdbms, table_prefix):
        source_bucket_uri_prefix = self.landing_bucket_uri_prefix(rdbms=rdbms, table=table)
        destination_project_dataset_table = (f"{self.config.environment.project}."
                                             f"{self.config.environment.raw_dataset}."
                                             f"{table_prefix}_{table.name}")
        create_external_bigquery_table_motif = CreateExternalBigQueryTableMotif(
            config=self.config,
            table=table,
            source_bucket_uri_prefix=source_bucket_uri_prefix,
            destination_project_dataset_table=destination_project_dataset_table
        )

        return create_external_bigquery_table_motif

    def build(self, movement_builder: Callable[[Table], MovementBase]) -> None:
        from airflow import DAG
        dags = list()
        for table in self.tables_service.tables():
            name = self.config.dag_parameters.dag_id + '.' + table.name
            kwargs = {**self.config.dag_parameters}
            del kwargs['dag_id']
            dag = DAG(dag_id=name, **kwargs)
            movement_builder(table).play(dag=dag)
            dags.append(dag)
        return dags
