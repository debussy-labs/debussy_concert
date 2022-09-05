from debussy_concert.core.movement.protocols import (
    PIngestionSourceToRawVaultStoragePhrase,
)
from debussy_concert.core.phrase.phrase_base import PhraseBase


class CreateOrUpdateRawTablePhrase(PhraseBase, PIngestionSourceToRawVaultStoragePhrase):
    # TODO: This is an incomplete implementation, only the create table is implemented
    def __init__(
        self,
        create_table_motif,
        check_table_exist_motif=None,
        update_table_motif=None,
        branching_motif=None,
        name=None,
    ) -> None:
        self.check_table_exist_motif = check_table_exist_motif
        self.create_table_motif = create_table_motif
        self.update_table_motif = update_table_motif
        self.branching_motif = branching_motif
        motifs = [
            self.check_table_exist_motif,
            self.create_table_motif,
            self.update_table_motif,
            self.branching_motif,
        ]
        super().__init__(name=name, motifs=motifs)

    def setup(self, table_uri):
        self.create_table_motif.setup(table_uri=table_uri)
        # self.check_table_exist_motif.setup(
        #     table_uri=destination_table_uri
        # )
        # self.branching_motif.setup(
        #     condition_motif=self.check_table_exist_motif,
        #     case_true_motif=self.update_table_motif,
        #     case_false_motif=self.create_table_motif
        # )
        return self

    def build(self, workflow_dag, movement_group):
        phrase_group = self.workflow_service.phrase_group(
            group_id=self.name, workflow_dag=workflow_dag, movement_group=movement_group
        )
        return self.create_table_motif.build(workflow_dag, phrase_group)
