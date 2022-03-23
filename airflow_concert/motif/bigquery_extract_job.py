from typing import Optional
from airflow_concert.motif.motif_base import MotifBase
from airflow_concert.motif.mixins.bigquery_job import BigQueryJobMixin, BigQueryTimePartitioning
from airflow_concert.phrase.protocols import PExecuteQueryMotif


class BigQueryExtractJobMotif(MotifBase, BigQueryJobMixin, PExecuteQueryMotif):
    def __init__(self, config=None, name=None):
        super().__init__(name=name, config=config)

    def setup(self):
        pass

    def build(self, dag, phrase_group):
        bigquery_job_operator = self.insert_job_operator(
            dag, phrase_group,
            self.extract_configuration(
            )
        )
        return bigquery_job_operator
