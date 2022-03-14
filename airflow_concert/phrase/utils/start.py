from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.motif.start import StartMotif


class StartPhrase(PhraseBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(
            name=name,
            motifs=[StartMotif(config=config)]
        )
