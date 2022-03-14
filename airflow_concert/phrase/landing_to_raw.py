from airflow_concert.movement.protocols import PLandingStorageToDataWarehouseRawPhrase
from airflow_concert.phrase.protocols import PMergeLandingToRawMotif
from airflow_concert.phrase.phrase_base import PhraseBase


class GcsLandingToBigQueryRawPhrase(PhraseBase, PLandingStorageToDataWarehouseRawPhrase):
    def __init__(
        self,
        merge_landing_to_raw_motif: PMergeLandingToRawMotif,
        name=None
    ) -> None:
        motifs = [merge_landing_to_raw_motif]
        super().__init__(name=name,
                         motifs=motifs)
