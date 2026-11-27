from debussy.core.phrase.phrase_base import PhraseBase


class StartPhrase(PhraseBase):
    def __init__(self, start_motif=None, name=None) -> None:
        if start_motif is None:
            # The default motif builds Airflow tasks, so it is imported here and
            # not at module level: the core must import without Airflow.
            from debussy.backends.airflow.motifs import StartMotif

            start_motif = StartMotif()
        self.start_motif = start_motif
        super().__init__(name=name, motifs=[self.start_motif])
