from debussy_concert.core.phrase.phrase_base import PhraseBase
from debussy_concert.core.motif.start import StartMotif


class StartPhrase(PhraseBase):
    def __init__(self, config, start_motif=None, name=None) -> None:
        self.start_motif = start_motif or StartMotif(config=config)
        super().__init__(
            name=name,
            motifs=[self.start_motif]
        )
