from airflow_concert.movement.protocols import PLandingStorageToDataWarehouseRawPhrase
from airflow_concert.phrase.protocols import PMergeLandingToRawMotif, PCreateExternalTableMotif
from airflow_concert.phrase.phrase_base import PhraseBase


class LandingStorageToDataWarehouseRawPhrase(PhraseBase, PLandingStorageToDataWarehouseRawPhrase):
    def __init__(
        self,
        create_external_table_motif: PCreateExternalTableMotif,
        merge_landing_to_raw_motif: PMergeLandingToRawMotif,
        name=None
    ) -> None:
        motifs = [create_external_table_motif, merge_landing_to_raw_motif]
        super().__init__(name=name,
                         motifs=motifs)
