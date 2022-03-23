from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.motif.start import StartMotif


class StartPhrase(PhraseBase):
    def __init__(self, config, start_motif=None, name=None) -> None:
        self.start_motif = start_motif or StartMotif(config=config)
        super().__init__(
            name=name,
            motifs=[self.start_motif]
        )
