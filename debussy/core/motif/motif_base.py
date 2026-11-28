from abc import ABC
from typing import Callable, Protocol, TypeVar
import inject

from debussy.core.entities.protocols import PWorkflowDag, PMotifGroup
from debussy.core.config.config_composition import ConfigComposition
from debussy.core.service.workflow.protocol import PWorkflowService

ConfigCompositionType = TypeVar("ConfigCompositionType", bound=ConfigComposition)


class PMotif(Protocol):
    name: str
    workflow_service: PWorkflowService

    @property
    def config(self) -> ConfigCompositionType:
        pass

    def play(self, *args, **kwargs) -> PMotifGroup:
        pass

    def build(self, dag, phrase_group) -> PMotifGroup:
        pass


class PClusterMotifMixin(PMotif, Protocol):
    cluster_name: str
    cluster_config: dict


class MotifBase(PMotif, ABC):
    """Base class of every motif.

    A motif either describes its work as engine-neutral tasks, by defining
    ``tasks()`` (see ``debussy.core.tasks.PTaskMotif``), or overrides
    ``build()`` to build engine-specific tasks itself. Only motifs with
    ``tasks()`` can run on any backend.
    """

    #: Whether ``build()`` puts the tasks from ``tasks()`` in a group named
    #: after the motif. A motif with a single task can set it to False to build
    #: that task directly in the phrase.
    group_tasks = True

    @inject.params(config=ConfigComposition, workflow_service=PWorkflowService)
    def __init__(
        self,
        *,
        workflow_service: PWorkflowService,
        config: ConfigComposition,
        name=None,
    ) -> None:
        self.workflow_service = workflow_service
        self._config = config
        self.name = name or self.__class__.__name__

    @property
    def config(self) -> ConfigCompositionType:
        return self._config

    def play(self, *args, **kwargs):
        return self.build(*args, **kwargs)

    def _build(
        self,
        workflow_dag,
        phrase_group,
        task_builder: Callable[[PWorkflowDag, PMotifGroup], None],
    ):
        motif_group = self.workflow_service.motif_group(
            group_id=self.name, workflow_dag=workflow_dag, phrase_group=phrase_group
        )
        task_builder(workflow_dag, motif_group)
        return motif_group

    def build(self, workflow_dag, phrase_group) -> PMotifGroup:
        tasks = getattr(self, "tasks", None)
        if tasks is None:
            raise NotImplementedError(
                f"{type(self).__name__} must define tasks() or override build()"
            )
        return self.workflow_service.build_tasks(
            group_id=self.name,
            specs=tasks(),
            workflow_dag=workflow_dag,
            phrase_group=phrase_group,
            grouped=self.group_tasks,
        )
