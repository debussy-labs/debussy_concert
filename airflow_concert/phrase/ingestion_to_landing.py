from airflow_concert.movement.protocols import PIngestionSourceToLandingStoragePhrase
from airflow_concert.phrase.protocols import PExportTableMotif
from airflow_concert.phrase.phrase_base import PhraseBase


class IngestionToLandingPhrase(PhraseBase, PIngestionSourceToLandingStoragePhrase):
    def __init__(
        self,
        export_table_motif: PExportTableMotif,
        name=None
    ) -> None:
        motifs = [export_table_motif]
        super().__init__(name=name,
                         motifs=motifs)
