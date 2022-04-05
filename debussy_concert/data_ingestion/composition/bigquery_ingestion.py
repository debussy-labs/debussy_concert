
from debussy_concert.data_ingestion.config.bigquery_data_ingestion import ConfigBigQueryDataIngestion
from debussy_concert.data_ingestion.config.movement_parameters.bigquery import BigQueryDataIngestionMovementParameters

from debussy_concert.core.composition.composition_base import CompositionBase
from debussy_concert.data_ingestion.movement.data_ingestion import DataIngestionMovement


from debussy_concert.data_ingestion.phrase.ingestion_to_landing import IngestionSourceToLandingStoragePhrase
from debussy_concert.data_ingestion.phrase.landing_to_raw import LandingStorageExternalTableToDataWarehouseRawPhrase
from debussy_concert.core.phrase.utils.start import StartPhrase
from debussy_concert.core.phrase.utils.end import EndPhrase

from debussy_concert.data_ingestion.motif.export_table import ExportBigQueryQueryMotif
from debussy_concert.data_ingestion.motif.create_external_table import CreateExternalBigQueryTableMotif
from debussy_concert.data_ingestion.motif.merge_table import MergeAppendBigQueryTableMotif


class BigQueryIngestionComposition(CompositionBase):
    config: ConfigBigQueryDataIngestion

    def __init__(self,):
        super().__init__()

    def auto_play(self):
        builder_fn = self.bigquery_ingestion_movement_builder
        dag = self.play(builder_fn)
        return dag

    def bigquery_ingestion_movement_builder(
            self, movement_parameters: BigQueryDataIngestionMovementParameters) -> DataIngestionMovement:
        start_phrase = StartPhrase()
        export_motif = ExportBigQueryQueryMotif(
            partition='execution_date={{ execution_date }}',
            extract_query=movement_parameters.extract_sql_query,
            extract_data_kwargs=movement_parameters.output_config)
        ingestion_to_landing_phrase = IngestionSourceToLandingStoragePhrase(export_data_to_storage_motif=export_motif)
        gcs_landing_to_bigquery_raw_phrase = self.gcs_landing_to_bigquery_raw_phrase(movement_parameters)
        end_phrase = EndPhrase()

        name = f'DataIngestionMovement_{movement_parameters.name}'
        movement = DataIngestionMovement(
            name=name,
            start_phrase=start_phrase,
            ingestion_source_to_landing_storage_phrase=ingestion_to_landing_phrase,
            landing_storage_to_data_warehouse_raw_phrase=gcs_landing_to_bigquery_raw_phrase,
            end_phrase=end_phrase
        )
        movement.setup(movement_parameters)
        return movement

    def gcs_landing_to_bigquery_raw_phrase(
            self, movement_parameters: BigQueryDataIngestionMovementParameters
    ) -> LandingStorageExternalTableToDataWarehouseRawPhrase:
        create_external_bigquery_table_motif = self.create_external_bigquery_table_motif()
        merge_bigquery_table_motif = self.merge_bigquery_table_motif(movement_parameters)
        gcs_landing_to_bigquery_raw_phrase = LandingStorageExternalTableToDataWarehouseRawPhrase(
            name='Landing_to_Raw_Phrase',
            create_external_table_motif=create_external_bigquery_table_motif,
            merge_table_motif=merge_bigquery_table_motif
        )
        return gcs_landing_to_bigquery_raw_phrase

    def merge_bigquery_table_motif(
            self, movement_parameters: BigQueryDataIngestionMovementParameters) -> MergeAppendBigQueryTableMotif:
        merge_bigquery_table_motif = MergeAppendBigQueryTableMotif(
            movement_parameters=movement_parameters
        )
        return merge_bigquery_table_motif

    def create_external_bigquery_table_motif(
            self) -> CreateExternalBigQueryTableMotif:
        create_external_bigquery_table_motif = CreateExternalBigQueryTableMotif()
        return create_external_bigquery_table_motif
