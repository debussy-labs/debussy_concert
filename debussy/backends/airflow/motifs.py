"""Operators behind the start motif (``debussy.core.motif.lifecycle``)."""

from datetime import datetime, timezone

from airflow.sdk import BaseOperator


class StartOperator(BaseOperator):
    """First task of a phase: returns the run's dates as its XCom value."""

    def __init__(self, phase, **kwargs):
        super().__init__(task_id=f"begin_{phase}", **kwargs)
        self.phase = phase

    def execute(self, context):
        def isoformat(value):
            return value.isoformat() if value else None

        return {
            "logical_date": isoformat(context.get("logical_date")),
            "data_interval_start": isoformat(context.get("data_interval_start")),
            "data_interval_end": isoformat(context.get("data_interval_end")),
            "current_date": datetime.now(timezone.utc).isoformat(),
        }
