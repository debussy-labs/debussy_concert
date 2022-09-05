from typing import Type
import inject
from debussy_concert.core.service.workflow.protocol import PWorkflowService
from debussy_concert.core.config.config_composition import ConfigComposition


def inject_dependencies(
    workflow_service: PWorkflowService, config_composition: ConfigComposition
) -> None:
    def inject_fn(binder: inject.Binder):
        binder.bind(PWorkflowService, workflow_service)
        binder.bind(ConfigComposition, config_composition)

    inject.configure(inject_fn, bind_in_runtime=False)
