from debussy_concert.core.phrase.phrase_base import PhraseBase


class StorageToDestinationPhrase(PhraseBase):
    def __init__(self,
                 storage_to_destination_motif,
                 name=None
                 ):
        self.storage_to_destination_motif = storage_to_destination_motif
        motifs = [self.storage_to_destination_motif]
        super().__init__(motifs=motifs, name=name)

    def setup(self, storage_uri_prefix):
        self.storage_to_destination_motif.setup(
            storage_uri_prefix=storage_uri_prefix)
        return self
