from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.motif.end import EndMotif


class EndPhrase(PhraseBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(
            name=name,
            motifs=[EndMotif(config=config)]
        )
