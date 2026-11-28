"""The motifs every pipeline starts and ends with."""

import logging

from debussy.core.motif.motif_base import MotifBase
from debussy.core.tasks import TaskSpec


def log_input(**kwargs):
    """Log the pipeline's input (its composition config)."""
    logging.info(kwargs)


class StartMotif(MotifBase):
    """Marks the start of a pipeline and logs its configuration."""

    def tasks(self):
        return [
            TaskSpec(name="begin_dag", action="debussy.begin", params={"phase": "dag"}),
            TaskSpec(
                name="log_input", action=log_input, params={"config": self.config}
            ),
        ]


class EndMotif(MotifBase):
    """Marks the end of a pipeline."""

    group_tasks = False

    def tasks(self):
        return [TaskSpec(name=self.name, action="debussy.noop")]
