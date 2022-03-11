from airflow_concert.composition.composition_base import CompositionBase
from airflow_concert.movement.ingestion_to_landing import IngestionToLandingMovement
from airflow_concert.movement.landing_to_raw import GcsLandingToBigQueryRawMovement
from airflow_concert.movement.raw_to_trusted import BigQueryRawToBigQueryTrustedMovement
from airflow_concert.movement.utils.start import StartMovement
from airflow_concert.movement.utils.end import EndMovement
from airflow_concert.phrase.export_table import ExportMySqlTablePhrase
from airflow_concert.config.config_integration import ConfigIntegration
from airflow_concert.composer.composer_base import ComposerBase
from airflow_concert.entities.table import Table


class Debussy(ComposerBase):
    def __init__(self, config: ConfigIntegration):
        super().__init__(config)

    def rdbms_ingestion_composition(self, ingestion_to_landing, table: Table, rdbms):
        from airflow_concert.phrase.merge_table import MergeReplaceBigQueryPhrase
        origin_bucket = (f"gs://{self.config.environment.landing_bucket}/"
                         f"{rdbms}/{self.config.database}/{table.name}")
        merge_landing_to_raw = MergeReplaceBigQueryPhrase(
            config=self.config,
            table=table,
            destiny_dataset=self.config.environment.raw_dataset,
            origin_bucket=origin_bucket
        )
        movements = [
            StartMovement(config=self.config),
            ingestion_to_landing,
            GcsLandingToBigQueryRawMovement(
                name='Landing_to_Raw_Movement',
                merge_landing_to_raw=merge_landing_to_raw
            ),
            BigQueryRawToBigQueryTrustedMovement(name='Raw_to_Trusted_Movement', config=self.config),
            EndMovement(config=self.config)

        ]
        name = f'Composition_{table.name}'
        ingestion = CompositionBase(name=name, config=self.config, movements=movements)
        return ingestion

    def mysql_composition(self, table: Table) -> None:
        ingestion_to_landing = IngestionToLandingMovement(ExportMySqlTablePhrase(config=self.config, table=table))
        return self.rdbms_ingestion_composition(ingestion_to_landing, table, rdbms='mysql')

    def build(self, composition_callable, globals) -> None:
        from airflow import DAG
        for table in self.tables_service.tables():
            name = self.config.dag_parameters.dag_id + '.' + table.name
            kwargs = {**self.config.dag_parameters}
            del kwargs['dag_id']
            dag = DAG(dag_id=name, **kwargs)
            composition = composition_callable(table)
            composition.build(dag=dag)
            globals[name] = dag
