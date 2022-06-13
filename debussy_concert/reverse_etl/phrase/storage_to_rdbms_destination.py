from debussy_concert.core.phrase.phrase_base import PhraseBase


class StorageToRdbmsDestinationPhrase(PhraseBase):
    def __init__(self,
                 storage_to_rdbms_destination_motif,
                 name=None
                 ):
        self.storage_to_rdbms_destination_motif = storage_to_rdbms_destination_motif
        motifs = [self.storage_to_rdbms_destination_motif]
        super().__init__(motifs=motifs, name=name)

    def setup(self, reverse_etl_bucket_uri, destination_table):
        self.storage_to_rdbms_destination_motif.setup(bucket_file_path = reverse_etl_bucket_uri,
            destination_table=destination_table)
        return self
