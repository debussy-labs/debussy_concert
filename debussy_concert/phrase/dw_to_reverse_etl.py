from debussy_concert.phrase.phrase_base import PhraseBase


class DataWarehouseToReverseEtlPhrase(PhraseBase):
    def __init__(
        self,
        dw_to_reverse_etl_motif,
        name=None
    ) -> None:
        self.dw_to_reverse_etl_motif = dw_to_reverse_etl_motif
        motifs = [self.dw_to_reverse_etl_motif]
        super().__init__(name=name,
                         motifs=motifs)

    def setup(self, reverse_etl_query, reverse_etl_table_uri):
        self.dw_to_reverse_etl_motif.setup(
            sql_query=reverse_etl_query,
            destination_table=reverse_etl_table_uri,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",)
