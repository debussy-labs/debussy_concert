from debussy_concert.movement.protocols import PIngestionSourceToLandingStoragePhrase
from debussy_concert.phrase.protocols import PExportDataToStorageMotif
from debussy_concert.phrase.phrase_base import PhraseBase


class IngestionSourceToLandingStoragePhrase(PhraseBase, PIngestionSourceToLandingStoragePhrase):
    def __init__(
        self,
        export_data_to_storage_motif: PExportDataToStorageMotif,
        name=None
    ) -> None:
        self.export_data_to_storage_motif = export_data_to_storage_motif
        motifs = [self.export_data_to_storage_motif]
        super().__init__(name=name,
                         motifs=motifs)

    def setup(self, destination_storage_uri):
        self.export_data_to_storage_motif.setup(
            destination_storage_uri=destination_storage_uri)
        return self
