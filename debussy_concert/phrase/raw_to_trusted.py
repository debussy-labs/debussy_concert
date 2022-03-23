from debussy_concert.movement.protocols import PDataWarehouseRawToTrustedPhrase
from debussy_concert.phrase.protocols import PRawToTrustedMotif
from debussy_concert.phrase.phrase_base import PhraseBase


class DataWarehouseRawToTrustedPhrase(PhraseBase, PDataWarehouseRawToTrustedPhrase):
    def __init__(
        self,
        raw_to_trusted_motif: PRawToTrustedMotif,
        name=None
    ) -> None:
        motifs = [raw_to_trusted_motif]
        super().__init__(name=name,
                         motifs=motifs)
