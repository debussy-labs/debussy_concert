from debussy.core.motif.lifecycle import StartMotif
from debussy.core.phrase.phrase_base import PhraseBase


class StartPhrase(PhraseBase):
    def __init__(self, start_motif=None, name=None) -> None:
        self.start_motif = start_motif or StartMotif()
        super().__init__(name=name, motifs=[self.start_motif])
