from airflow_concert.phrase.phrase_base import PhraseBase


class GcsLandingToBigQueryRawPhrase(PhraseBase):

    def __init__(self, merge_landing_to_raw, name=None) -> None:
        motifs = [merge_landing_to_raw]
        super().__init__(name=name,
                         motifs=motifs)
