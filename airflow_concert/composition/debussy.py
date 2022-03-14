from typing import Callable
from airflow_concert.movement.movement_base import MovementBase
from airflow_concert.phrase.ingestion_to_landing import IngestionToLandingPhrase
from airflow_concert.phrase.landing_to_raw import GcsLandingToBigQueryRawPhrase
from airflow_concert.phrase.raw_to_trusted import BigQueryRawToBigQueryTrustedPhrase
from airflow_concert.phrase.utils.start import StartPhrase
from airflow_concert.phrase.utils.end import EndPhrase
from airflow_concert.motif.export_table import ExportMySqlTableMotif
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.entities.table import Table


class Debussy(CompositionBase):
    def __init__(self, config: ConfigIntegration):
        super().__init__(config)

    def rdbms_ingestion_movement(self, ingestion_to_landing, table: Table, rdbms):
        from airflow_concert.motif.merge_table import MergeReplaceBigQueryMotif
        origin_bucket = (f"gs://{self.config.environment.landing_bucket}/"
                         f"{rdbms}/{self.config.database}/{table.name}")
        merge_landing_to_raw = MergeReplaceBigQueryMotif(
            config=self.config,
            table=table,
            destiny_dataset=self.config.environment.raw_dataset,
            origin_bucket=origin_bucket
        )
        phrases = [
            StartPhrase(config=self.config),
            ingestion_to_landing,
            GcsLandingToBigQueryRawPhrase(
                name='Landing_to_Raw_Phrase',
                merge_landing_to_raw=merge_landing_to_raw
            ),
            BigQueryRawToBigQueryTrustedPhrase(name='Raw_to_Trusted_Phrase', config=self.config),
            EndPhrase(config=self.config)

        ]
        name = f'Movement_{table.name}'
        ingestion = MovementBase(name=name, phrases=phrases)
        return ingestion

    def mysql_movement(self, table: Table) -> None:
        ingestion_to_landing = IngestionToLandingPhrase(ExportMySqlTableMotif(config=self.config, table=table))
        return self.rdbms_ingestion_movement(ingestion_to_landing, table, rdbms='mysql')

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
