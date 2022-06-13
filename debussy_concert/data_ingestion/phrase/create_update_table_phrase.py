from debussy_concert.core.movement.protocols import PIngestionSourceToRawVaultStoragePhrase
from debussy_concert.core.phrase.phrase_base import PhraseBase


class CreateOrUpdateRawTablePhrase(PhraseBase, PIngestionSourceToRawVaultStoragePhrase):
    def __init__(
        self,
        create_or_update_table_motif,
        # create table motif
        # check table exist
        # update table motif
        name=None
    ) -> None:
        self.create_or_update_table_motif = create_or_update_table_motif
        motifs = [self.create_or_update_table_motif]
        super().__init__(name=name,
                         motifs=motifs)

    def setup(self, destination_table_uri):
        self.create_or_update_table_motif.setup(
            destination_table_uri=destination_table_uri)
        return self
