from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.motif.motif_base import MotifBase


class IngestionToLandingPhrase(PhraseBase):
    def __init__(self, export_table_motif: MotifBase, name=None) -> None:
        motifs = [export_table_motif]
        super().__init__(name=name,
                         motifs=motifs)
