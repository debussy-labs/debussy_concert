from debussy.core.phrase.phrase_base import PhraseBase


class EndPhrase(PhraseBase):
    def __init__(self, end_motif=None, name=None) -> None:
        if end_motif is None:
            # The default motif builds Airflow tasks, so it is imported here and
            # not at module level: the core must import without Airflow.
            from debussy.backends.airflow.motifs import EndMotif

            end_motif = EndMotif()
        self.end_motif = end_motif
        super().__init__(name=name, motifs=[self.end_motif])
