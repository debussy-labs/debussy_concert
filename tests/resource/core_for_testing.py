from dataclasses import dataclass
from debussy_concert.core.phrase.phrase_base import PhraseBase
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.core.service.workflow.protocol import (
    PMotifGroup,
    PPhraseGroup,
    PMovementGroup,
    PWorkflowDag,
)


class TaskMixin:
    def __rshift__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError()
        self.next = other

    def __lshift__(self, other):
        if not isinstance(other, self.super().__class__):
            raise TypeError()
        self.next = other


class DummyPhrase(PhraseBase, TaskMixin):
    _setup = {}

    def setup(self, **kwargs):
        self._setup = kwargs


class DummyMotif(MotifBase, TaskMixin):
    _setup = {}

    def setup(self, **kwargs):
        self._setup = kwargs

    def build(self, workflow_dag, phrase_group) -> PMotifGroup:
        self.build_args = {"workflow_dag": workflow_dag, "phrase_group": phrase_group}
        return self


@dataclass
class DummyMotifGroup(PMotifGroup, TaskMixin):
    group_id: str
    workflow_dag: str
    phrase_group: str


@dataclass
class DummyPhraseGroup(PPhraseGroup, TaskMixin):
    group_id: str
    workflow_dag: str
    movement_group: str


@dataclass
class DummyMovementGroup(PMovementGroup, TaskMixin):
    group_id: str
    workflow_dag: str


class DummyDag(PWorkflowDag):
    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs


def create_empty_phrase(name):
    motif = DummyMotif(name=f"{name}_motif")
    return DummyPhrase(name=name, motifs=[motif])
