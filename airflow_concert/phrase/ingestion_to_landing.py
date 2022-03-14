from airflow_concert.movement.protocols import PIngestionSourceToLandingStoragePhrase
from airflow_concert.phrase.protocols import PExportDataToStorageMotif
from airflow_concert.phrase.phrase_base import PhraseBase


class IngestionSourceToLandingStoragePhrase(PhraseBase, PIngestionSourceToLandingStoragePhrase):
    def __init__(
        self,
        export_data_to_storage_motif: PExportDataToStorageMotif,
        name=None
    ) -> None:
        motifs = [export_data_to_storage_motif]
        super().__init__(name=name,
                         motifs=motifs)
