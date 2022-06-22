from typing import Callable

from debussy_concert.core.movement.movement_base import PMovement
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase
from debussy_concert.data_ingestion.config.rdbms_data_ingestion import ConfigRdbmsDataIngestion
from debussy_concert.data_ingestion.config.movement_parameters.rdbms_data_ingestion import RdbmsDataIngestionMovementParameters
from debussy_concert.data_ingestion.composition.base import DataIngestionBase
from debussy_concert.data_ingestion.movement.data_ingestion import DataIngestionMovement
from debussy_concert.data_ingestion.phrase.ingestion_to_raw_vault import IngestionSourceToRawVaultStoragePhrase
from debussy_concert.data_ingestion.motif.export_table import DataprocExportRdbmsTableToGcsMotif


class RdbmsIngestionComposition(DataIngestionBase):
    config: ConfigRdbmsDataIngestion

    def __init__(self):
        super().__init__()

    def auto_play(self):
        rdbms_builder_fn = self.rdbms_builder_fn()
        dag = self.play(rdbms_builder_fn)
        return dag

    def mysql_ingestion_movement_builder(
            self, movement_parameters: RdbmsDataIngestionMovementParameters) -> DataIngestionMovement:
        ingestion_to_raw_vault_phrase = self.mysql_ingestion_to_raw_vault_phrase(movement_parameters)
        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=ingestion_to_raw_vault_phrase
        )

    def mysql_ingestion_to_raw_vault_phrase(self, movement_parameters: RdbmsDataIngestionMovementParameters):

        mysql_jdbc_driver = "com.mysql.cj.jdbc.Driver"
        jdbc_url = "jdbc:mysql://{host}:{port}/" + self.config.source_name

        export_mysql_to_gcs_motif = DataprocExportRdbmsTableToGcsMotif(
            movement_parameters=movement_parameters, gcs_partition=movement_parameters.data_partitioning.gcs_partition_schema, jdbc_driver=mysql_jdbc_driver, jdbc_url=jdbc_url)

        ingestion_to_raw_vault_phrase = IngestionSourceToRawVaultStoragePhrase(
            export_data_to_storage_motif=export_mysql_to_gcs_motif
        )

        return ingestion_to_raw_vault_phrase

    def mssql_ingestion_movement_builder(
            self, movement_parameters: RdbmsDataIngestionMovementParameters) -> DataIngestionMovement:
        raise NotImplementedError("MS SQL ingestion is not implemented yet")

    def postgresql_ingestion_movement_builder(
            self, movement_parameters: RdbmsDataIngestionMovementParameters) -> DataIngestionMovement:
        raise NotImplementedError("PostgreSQL ingestion is not implemented yet")

    def rdbms_builder_fn(self) -> Callable[[RdbmsDataIngestionMovementParameters], PMovement]:
        map_ = {
            'mysql': self.mysql_ingestion_movement_builder,
            'mssql': self.mssql_ingestion_movement_builder,
            'postgresql': self.postgresql_ingestion_movement_builder
        }
        rdbms_name = self.config.source_type.lower()
        builder = map_.get(rdbms_name)
        if not builder:
            raise NotImplementedError(f"Invalid rdbms: {rdbms_name} not implemented")
        return builder
