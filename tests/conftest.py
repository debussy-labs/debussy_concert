from pytest import fixture
import inject

from debussy_concert.pipeline.reverse_etl.config.reverse_etl import ConfigReverseEtl
from debussy_concert.core.service.workflow.protocol import PWorkflowService
from debussy_concert.core.config.config_composition import ConfigComposition
from tests.resource.workflow_for_testing import WorkflowServiceForTesting


@fixture(scope="session")
def workflow_service():
    return WorkflowServiceForTesting()


@fixture(scope="session")
def config_composition_for_testing():
    return ConfigReverseEtl.load_from_file(
        composition_config_file_path="tests/resource/yaml/reverse_etl/composition.yaml",
        env_file_path="tests/resource/yaml/reverse_etl/environment.yaml",
    )


@fixture(scope="session")
def inject_testing(workflow_service, config_composition_for_testing):
    def inject_fn(binder: inject.Binder):
        binder.bind(PWorkflowService, workflow_service)
        binder.bind(ConfigComposition, config_composition_for_testing)

    inject.clear_and_configure(inject_fn, bind_in_runtime=False)
