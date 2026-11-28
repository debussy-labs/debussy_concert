"""``MotifBase.build`` hands a motif's ``tasks()`` to the workflow service."""

import pytest

from debussy.core.motif.motif_base import MotifBase
from debussy.core.tasks import PTaskMotif, TaskSpec
from tests.resource.workflow_for_testing import WorkflowServiceForTesting


def _noop(**kwargs):
    return kwargs


class TaskMotif(MotifBase):
    def tasks(self):
        return [
            TaskSpec(name="first", action=_noop),
            TaskSpec(name="second", action="storage.copy"),
        ]


class SingleTaskMotif(MotifBase):
    group_tasks = False

    def tasks(self):
        return [TaskSpec(name=self.name, action=_noop)]


class NeitherMotif(MotifBase):
    pass


def _motif(cls, **kwargs):
    return cls(workflow_service=WorkflowServiceForTesting(), config=None, **kwargs)


def test_build_passes_the_tasks_to_the_workflow_service():
    motif = _motif(TaskMotif, name="Copy")
    assert isinstance(motif, PTaskMotif)
    group = motif.build("dag", "phrase")
    assert group.group_id == "Copy"
    assert group.workflow_dag == "dag"
    assert group.phrase_group == "phrase"
    assert [spec.name for spec in group.specs] == ["first", "second"]
    assert group.grouped is True


def test_group_tasks_is_passed_on():
    group = _motif(SingleTaskMotif).build("dag", "phrase")
    assert group.grouped is False


def test_a_motif_needs_tasks_or_build():
    motif = _motif(NeitherMotif)
    assert not isinstance(motif, PTaskMotif)
    with pytest.raises(NotImplementedError, match="NeitherMotif must define tasks"):
        motif.build("dag", "phrase")
