from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.config.reverse_etl import ConfigReverseEtl
from airflow_concert.entities.table import Table


class DataWarehouseReverseEtlToTempToStoragePhrase(PhraseBase):
    def __init__(
        self,
        datawarehouse_reverse_etl_to_temp_table_motif,
        export_temp_table_to_storage_motif,
        name=None
    ) -> None:
        self.datawarehouse_reverse_etl_to_temp_table_motif = datawarehouse_reverse_etl_to_temp_table_motif
        self.export_temp_table_to_storage_motif = export_temp_table_to_storage_motif
        motifs = [self.datawarehouse_reverse_etl_to_temp_table_motif,
                  self.export_temp_table_to_storage_motif]
        super().__init__(name=name,
                         motifs=motifs)

    @property
    def temp_table_uri(self):
        return (f"{self.config.environment.project}."
                f"{self.config.environment.temp_dataset}."
                f"{self.config.name}_{self.table.name}")

    def setup(self, config: ConfigReverseEtl, table: Table, extract_query, storage_uri_prefix):
        self.config = config
        self.table = table
        self.datawarehouse_reverse_etl_to_temp_table_motif.setup(
            extract_query=extract_query,
            destiny_table_uri=self.temp_table_uri)
        self.export_temp_table_to_storage_motif.setup(
            origin_table_uri=self.temp_table_uri,
            storage_uri_prefix=storage_uri_prefix)
