from typing import Optional, Union, List
from debussy_concert.motif.motif_base import MotifBase
from debussy_concert.motif.mixins.bigquery_job import BigQueryJobMixin, BigQueryTimePartitioning
from debussy_concert.phrase.protocols import PExecuteQueryMotif


class BigQueryExtractJobMotif(MotifBase, BigQueryJobMixin):
    def __init__(self,
                 source_table_uri: Optional[str] = None,
                 destination_uris: Optional[Union[List[str], str]] = None,
                 field_delimiter: Optional[str] = ',',
                 destination_format: Optional[str] = 'CSV',
                 config=None, name=None):
        super().__init__(name=name, config=config)
        self.source_table_uri = source_table_uri
        self.destination_uris = destination_uris
        self.field_delimiter = field_delimiter
        self.destination_format = destination_format

    def setup(self,
              source_table_uri: str,
              destination_uris: Union[List[str], str]):
        self.source_table_uri = source_table_uri
        self.destination_uris = destination_uris

    def build(self, dag, phrase_group):
        if self.source_table_uri is None:
            raise ValueError("source_table_uri cant be none. initialize on init or setup")
        if self.destination_uris is None:
            raise ValueError("destination_uris cant be none. initialize on init or setup")
        bigquery_job_operator = self.insert_job_operator(
            dag, phrase_group,
            self.extract_configuration(
                self.source_table_uri,
                self.destination_uris,
                self.field_delimiter,
                self.destination_format
            )
        )
        return bigquery_job_operator
