from typing import Protocol, Sequence
import inject
from debussy_concert.core.config.config_composition import ConfigComposition
from debussy_concert.core.entities.protocols import PPhraseGroup
from debussy_concert.core.motif.motif_base import PMotif
from debussy_concert.core.service.workflow.protocol import PWorkflowService


class PPhrase(Protocol):
    motifs: Sequence[PMotif]

    def build(self, dag, parent_task_group) -> PPhraseGroup:
        pass


class PhraseBase(PPhrase):
    @inject.autoparams()
    def __init__(
            self, *,
            config: ConfigComposition,
            workflow_service: PWorkflowService,
            motifs: Sequence[PMotif] = None,
            name=None) -> None:
        self.name = name or self.__class__.__name__
        self.config = config
        self.motifs = motifs or list()
        self.workflow_service = workflow_service

    def add_motif(self, motif):
        self.motifs.append(motif)

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def build(self, workflow_dag, movement_group):
        phrase_group = self.workflow_service.phrase_group(
            group_id=self.name, workflow_dag=workflow_dag, movement_group=movement_group)
        current_task = self.motifs[0].build(workflow_dag, phrase_group)
        for motif in self.motifs[1:]:
            motif_task = motif.build(workflow_dag, phrase_group)
            current_task >> motif_task
            current_task = motif_task
        return phrase_group
