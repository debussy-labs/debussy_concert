from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.data_ingestion.movement.data_ingestion import DataIngestionMovement
from debussy_concert.data_ingestion.phrase.ingestion_to_landing import IngestionSourceToLandingStoragePhrase
from debussy_concert.data_ingestion.phrase.landing_to_raw import LandingStorageExternalTableToDataWarehouseRawPhrase
from debussy_concert.data_ingestion.phrase.raw_to_trusted import DataWarehouseRawToTrustedPhrase
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase
from debussy_concert.data_ingestion.motif.export_table import ExportFullMySqlTableToGcsMotif
from debussy_concert.core.motif.bigquery_query_job import BigQueryQueryJobMotif
from debussy_concert.data_ingestion.motif.create_external_table import CreateExternalBigQueryTableMotif
from debussy_concert.data_ingestion.motif.merge_table import MergeBigQueryTableMotif
from debussy_concert.data_ingestion.config.rdbms_data_ingestion import ConfigRdbmsDataIngestion
from debussy_concert.data_ingestion.config.movement_parameters.rdbms_data_ingestion import RdbmsDataIngestionMovementParameters


class Debussy(CompositionBase):
    config: ConfigRdbmsDataIngestion

    def __init__(self):
        super().__init__()

    @property
    def table_prefix(self):
        return self.config.table_prefix

    def landing_bucket_uri_prefix(self, rdbms: str, movement_parameters: RdbmsDataIngestionMovementParameters):
        return (f"gs://{self.config.environment.landing_bucket}/"
                f"{rdbms}/{self.config.database}/{movement_parameters.name}")

    def landing_external_table_uri(self, movement_parameters: RdbmsDataIngestionMovementParameters):
        return (f"{self.config.environment.project}."
                f"{self.config.environment.landing_dataset}."
                f"{self.table_prefix}_{movement_parameters.name}")

    def raw_table_uri(self, movement_parameters: RdbmsDataIngestionMovementParameters):
        return (f"{self.config.environment.project}."
                f"{self.config.environment.raw_dataset}."
                f"{self.table_prefix}_{movement_parameters.name}")

    def mysql_movement_builder(self, movement_parameters: RdbmsDataIngestionMovementParameters) -> DataIngestionMovement:
        rdbms = 'mysql'
        export_mysql_to_gcs_motif = ExportFullMySqlTableToGcsMotif(
            movement_parameters=movement_parameters).setup(
            destination_storage_uri=self.landing_bucket_uri_prefix(
                rdbms=rdbms, movement_parameters=movement_parameters))
        ingestion_to_landing_phrase = IngestionSourceToLandingStoragePhrase(
            export_data_to_storage_motif=export_mysql_to_gcs_motif
        )
        return self.rdbms_ingestion_movement_builder(ingestion_to_landing_phrase, movement_parameters, rdbms=rdbms)

    def rdbms_ingestion_movement_builder(
            self, ingestion_to_landing_phrase,
            movement_parameters: RdbmsDataIngestionMovementParameters, rdbms: str) -> DataIngestionMovement:
        start_phrase = StartPhrase()
        gcs_landing_to_bigquery_raw_phrase = self.gcs_landing_to_bigquery_raw_phrase(movement_parameters, rdbms)
        end_phrase = EndPhrase()

        name = f'Movement_{movement_parameters.name}'
        ingestion = DataIngestionMovement(
            name=name,
            start_phrase=start_phrase,
            ingestion_source_to_landing_storage_phrase=ingestion_to_landing_phrase,
            landing_storage_to_data_warehouse_raw_phrase=gcs_landing_to_bigquery_raw_phrase,
            end_phrase=end_phrase
        )
        return ingestion

    def gcs_landing_to_bigquery_raw_phrase(
            self, movement_parameters: RdbmsDataIngestionMovementParameters,
            rdbms) -> LandingStorageExternalTableToDataWarehouseRawPhrase:
        create_external_bigquery_table_motif = self.create_external_bigquery_table_motif(movement_parameters, rdbms)
        merge_bigquery_table_motif = self.merge_bigquery_table_motif(movement_parameters)
        gcs_landing_to_bigquery_raw_phrase = LandingStorageExternalTableToDataWarehouseRawPhrase(
            name='Landing_to_Raw_Phrase',
            create_external_table_motif=create_external_bigquery_table_motif,
            merge_table_motif=merge_bigquery_table_motif
        )
        return gcs_landing_to_bigquery_raw_phrase

    def merge_bigquery_table_motif(
            self,
            movement_parameters: RdbmsDataIngestionMovementParameters) -> MergeBigQueryTableMotif:
        main_table = self.raw_table_uri(movement_parameters)
        delta_table = self.landing_external_table_uri(movement_parameters)
        merge_bigquery_table_motif = MergeBigQueryTableMotif(

            movement_parameters=movement_parameters).setup(
            main_table_uri=main_table,
            delta_table_uri=delta_table)
        return merge_bigquery_table_motif

    def create_external_bigquery_table_motif(self,
                                             movement_parameters: RdbmsDataIngestionMovementParameters,
                                             rdbms) -> CreateExternalBigQueryTableMotif:
        source_bucket_uri_prefix = self.landing_bucket_uri_prefix(rdbms=rdbms, movement_parameters=movement_parameters)
        destination_project_dataset_table = self.landing_external_table_uri(movement_parameters)
        create_external_bigquery_table_motif = CreateExternalBigQueryTableMotif().setup(
            source_bucket_uri_prefix=source_bucket_uri_prefix,
            destination_project_dataset_table=destination_project_dataset_table)
        return create_external_bigquery_table_motif

    @classmethod
    def create_from_yaml(cls, environment_config_yaml_filepath, composition_config_yaml_filepath) -> 'Debussy':
        config = ConfigRdbmsDataIngestion.load_from_file(
            composition_config_file_path=composition_config_yaml_filepath,
            env_file_path=environment_config_yaml_filepath
        )
        return cls(config=config)
