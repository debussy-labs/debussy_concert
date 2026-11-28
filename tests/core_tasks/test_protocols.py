from debussy.core.tasks import PTaskMotif, TaskSpec


class TaskMotif:
    name = "TaskMotif"

    def tasks(self):
        return [TaskSpec(name="run", action=print)]


class BuildOnlyMotif:
    name = "BuildOnlyMotif"

    def build(self, workflow_dag, phrase_group):
        pass


def test_task_motif_matches_the_protocol():
    assert isinstance(TaskMotif(), PTaskMotif)


def test_build_only_motif_does_not_match():
    assert not isinstance(BuildOnlyMotif(), PTaskMotif)
