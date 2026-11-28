"""The start and end motifs describe their work with engine-neutral tasks."""

import logging

from debussy.core.motif.lifecycle import EndMotif, StartMotif, log_input
from tests.resource.workflow_for_testing import WorkflowServiceForTesting


def _service_and_config():
    return {"workflow_service": WorkflowServiceForTesting(), "config": "the-config"}


def test_start_motif_tasks():
    motif = StartMotif(**_service_and_config())
    group = motif.build("dag", "phrase")
    begin, log = group.specs
    assert (begin.name, begin.action, dict(begin.params)) == (
        "begin_dag",
        "debussy.begin",
        {"phase": "dag"},
    )
    assert (log.name, log.action, dict(log.params)) == (
        "log_input",
        log_input,
        {"config": "the-config"},
    )
    assert group.grouped is True


def test_end_motif_is_a_single_ungrouped_task():
    motif = EndMotif(name="End", **_service_and_config())
    group = motif.build("dag", "phrase")
    (spec,) = group.specs
    assert (spec.name, spec.action) == ("End", "debussy.noop")
    assert group.grouped is False


def test_log_input_logs_its_arguments(caplog):
    with caplog.at_level(logging.INFO):
        log_input(config="c")
    assert "'config': 'c'" in caplog.text
