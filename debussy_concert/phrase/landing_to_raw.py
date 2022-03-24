from debussy_concert.movement.protocols import PLandingStorageToDataWarehouseRawPhrase
from debussy_concert.phrase.protocols import PMergeTableMotif, PCreateExternalTableMotif, PExecuteQueryMotif
from debussy_concert.phrase.phrase_base import PhraseBase


class LandingStorageExternalTableToDataWarehouseRawPhrase(PhraseBase, PLandingStorageToDataWarehouseRawPhrase):
    def __init__(
        self,
        create_external_table_motif: PCreateExternalTableMotif,
        merge_table_motif: PMergeTableMotif,
        name=None
    ) -> None:
        self.create_external_table_motif = create_external_table_motif
        self.merge_table_motif = merge_table_motif
        motifs = [self.create_external_table_motif, self.merge_table_motif]
        super().__init__(name=name,
                         motifs=motifs)

    def landing_external_table_uri(self):
        return (f"{self.config.environment.project}."
                f"{self.config.environment.landing_dataset}."
                f"{self.config.table_prefix}_{self.table.name}")

    def setup(self, config, table, source_storage_uri_prefix, datawarehouse_raw_uri):
        self.config = config
        self.table = table
        self.create_external_table_motif.setup(
            source_bucket_uri_prefix=source_storage_uri_prefix,
            destination_project_dataset_table=self.landing_external_table_uri)
        self.merge_table_motif.setup(
            main_table_uri=datawarehouse_raw_uri, delta_table_uri=self.landing_external_table_uri)
        return self
