from debussy.core.motif.lifecycle import EndMotif
from debussy.core.phrase.phrase_base import PhraseBase


class EndPhrase(PhraseBase):
    def __init__(self, end_motif=None, name=None) -> None:
        self.end_motif = end_motif or EndMotif()
        super().__init__(name=name, motifs=[self.end_motif])
