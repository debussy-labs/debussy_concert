from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.motif.end import EndMotif


class EndPhrase(PhraseBase):
    def __init__(self, config, end_motif=None, name=None) -> None:
        self.end_motif = end_motif or EndMotif(config=config)
        super().__init__(
            name=name,
            motifs=[self.end_motif]
        )
