from debussy_concert.core.phrase.phrase_base import PhraseBase


class DataLakehouseTransformationPhrase(PhraseBase):
    def __init__(self, run_transformation_motif, name=None) -> None:
        self.run_transformation_motif = run_transformation_motif
        motifs = [self.run_transformation_motif]
        super().__init__(name=name, motifs=motifs)

    def setup(self):
        self.run_transformation_motif.setup()
        return self

    def build(self, workflow_dag, movement_group):
        phrase_group = self.workflow_service.phrase_group(
            group_id=self.name, workflow_dag=workflow_dag, movement_group=movement_group
        )
        return self.run_transformation_motif.build(workflow_dag, phrase_group)
