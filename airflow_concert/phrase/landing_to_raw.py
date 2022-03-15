from airflow_concert.movement.protocols import PLandingStorageToDataWarehouseRawPhrase
from airflow_concert.phrase.protocols import PMergeTableMotif, PCreateExternalTableMotif, PExecuteQueryMotif
from airflow_concert.phrase.phrase_base import PhraseBase


class LandingStorageToDataWarehouseRawPhrase(PhraseBase, PLandingStorageToDataWarehouseRawPhrase):
    def __init__(
        self,
        create_external_table_motif: PCreateExternalTableMotif,
        merge_table_motif: PMergeTableMotif,
        name=None
    ) -> None:
        motifs = [create_external_table_motif, merge_table_motif]
        super().__init__(name=name,
                         motifs=motifs)
