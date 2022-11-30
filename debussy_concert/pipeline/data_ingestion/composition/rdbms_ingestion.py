from typing import Callable

from debussy_concert.core.movement.movement_base import PMovement
from debussy_concert.pipeline.data_ingestion.config.rdbms_data_ingestion import (
    ConfigRdbmsDataIngestion,
)
from debussy_concert.pipeline.data_ingestion.config.movement_parameters.rdbms_data_ingestion import (
    RdbmsDataIngestionMovementParameters,
)
from debussy_concert.pipeline.data_ingestion.composition.base import DataIngestionBase
from debussy_concert.pipeline.data_ingestion.movement.data_ingestion import (
    DataIngestionMovement,
)
from debussy_concert.pipeline.data_ingestion.phrase.ingestion_to_raw_vault import (
    IngestionSourceToRawVaultStoragePhrase,
)
from debussy_concert.pipeline.data_ingestion.motif.export_table import (
    DataprocExportRdbmsTableToGcsMotif,
    DataprocServerlessExportRdbmsTableToGcsMotif,
)


class RdbmsIngestionComposition(DataIngestionBase):
    config: ConfigRdbmsDataIngestion

    def __init__(self):
        super().__init__()
        self.dataproc_main_python_file_uri = self.config.dataproc_config[
            "pyspark_script"
        ]

    def auto_play(self):
        rdbms_builder_fn = self.rdbms_builder_fn()
        dag = self.play(rdbms_builder_fn)
        return dag

    def rdbms_builder_fn(
        self,
    ) -> Callable[[RdbmsDataIngestionMovementParameters], PMovement]:
        map_ = {
            "mysql": self.mysql_ingestion_movement_builder,
            "mssql": self.mssql_ingestion_movement_builder,
            "postgresql": self.postgresql_ingestion_movement_builder,
        }
        rdbms_name = self.config.source_type.lower()
        builder = map_.get(rdbms_name)
        if not builder:
            raise NotImplementedError(f"Invalid rdbms: {rdbms_name} not implemented")
        return builder

    def mysql_ingestion_movement_builder(
        self, movement_parameters: RdbmsDataIngestionMovementParameters
    ) -> DataIngestionMovement:

        mysql_jdbc_driver = "com.mysql.cj.jdbc.Driver"
        mysql_jdbc_url = "jdbc:mysql://{host}:{port}/" + self.config.source_name

        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.rdbms_ingestion_to_raw_vault_phrase(
                mysql_jdbc_driver, mysql_jdbc_url, movement_parameters
            ),
        )

    def mssql_ingestion_movement_builder(
        self, movement_parameters: RdbmsDataIngestionMovementParameters
    ) -> DataIngestionMovement:

        mssql_jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        mssql_jdbc_url = (
            "jdbc:sqlserver://{host}:{port};databaseName=" + self.config.source_name
        )

        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.rdbms_ingestion_to_raw_vault_phrase(
                mssql_jdbc_driver, mssql_jdbc_url, movement_parameters
            ),
        )

    def postgresql_ingestion_movement_builder(
        self, movement_parameters: RdbmsDataIngestionMovementParameters
    ) -> DataIngestionMovement:

        postgresql_jdbc_driver = "org.postgresql.Driver"
        postgresql_jdbc_url = (
            "jdbc:postgresql://{host}:{port}/" + self.config.source_name
        )

        return self.ingestion_movement_builder(
            movement_parameters=movement_parameters,
            ingestion_to_raw_vault_phrase=self.rdbms_ingestion_to_raw_vault_phrase(
                postgresql_jdbc_driver, postgresql_jdbc_url, movement_parameters
            ),
        )

    def rdbms_ingestion_to_raw_vault_phrase(
        self,
        jdbc_driver,
        jdbc_url,
        movement_parameters: RdbmsDataIngestionMovementParameters,
    ):

        map_ = {
            "serverless": self.dataproc_serverless_export_rdbms_table_to_gcs(
                jdbc_driver, jdbc_url, movement_parameters
            ),
            "managed": self.dataproc_managed_export_rdbms_table_to_gcs(
                jdbc_driver, jdbc_url, movement_parameters
            ),
        }
        dataproc_type = self.config.dataproc_config.get("type", "managed")
        export_rdbms_to_gcs_motif = map_.get(dataproc_type)
        if not export_rdbms_to_gcs_motif:
            raise NotImplementedError(
                f"Invalid dataproc type: {dataproc_type} not implemented"
            )
        ingestion_to_raw_vault_phrase = IngestionSourceToRawVaultStoragePhrase(
            export_data_to_storage_motif=export_rdbms_to_gcs_motif
        )
        return ingestion_to_raw_vault_phrase

    def dataproc_managed_export_rdbms_table_to_gcs(
        self,
        jdbc_driver,
        jdbc_url,
        movement_parameters: RdbmsDataIngestionMovementParameters,
    ):
        return DataprocExportRdbmsTableToGcsMotif(
            movement_parameters=movement_parameters,
            gcs_partition=movement_parameters.data_partitioning.gcs_partition_schema,
            jdbc_driver=jdbc_driver,
            jdbc_url=jdbc_url,
            main_python_file_uri=self.dataproc_main_python_file_uri,
        )

    def dataproc_serverless_export_rdbms_table_to_gcs(
        self,
        jdbc_driver,
        jdbc_url,
        movement_parameters: RdbmsDataIngestionMovementParameters,
    ):
        return DataprocServerlessExportRdbmsTableToGcsMotif(
            movement_parameters=movement_parameters,
            gcs_partition=movement_parameters.data_partitioning.gcs_partition_schema,
            jdbc_driver=jdbc_driver,
            jdbc_url=jdbc_url,
            main_python_file_uri=self.dataproc_main_python_file_uri,
        )
