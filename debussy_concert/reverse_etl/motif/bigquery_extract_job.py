from asyncio.log import logger
import logging
from typing import Optional, Union, List
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.core.motif.mixins.bigquery_job import BigQueryJobMixin


class BigQueryExtractJobMotif(MotifBase, BigQueryJobMixin):
    def __init__(self,
                 field_delimiter: Optional[str] = ',',
                 destination_format: Optional[str] = 'CSV',
                 gcp_conn_id='google_cloud_default',
                 name=None):
        super().__init__(name=name)
        self.field_delimiter = field_delimiter
        self.destination_format = destination_format
        self.gcp_conn_id = gcp_conn_id

    def setup(self,
              source_table_uri: str,
              destination_uris: Union[List[str], str]):
        self.source_table_uri = source_table_uri
        self.destination_uris = destination_uris
        return self

    def build(self, dag, phrase_group):
        if self.source_table_uri is None:
            raise ValueError("source_table_uri cant be none. initialize on setup")
        if self.destination_uris is None:
            raise ValueError("destination_uris cant be none. initialize on setup")
        #logging.info(f"SOURCE TABLE URI: {self.source_table_uri}")
        #logging.info(f"self.destination_uris: {self.destination_uris}")
        #logging.info(f"self.destination_format: {self.destination_format}")
        bigquery_job_operator = self.insert_job_operator(
            dag, phrase_group,
            self.extract_configuration(
                self.source_table_uri,
                self.destination_uris,
                self.field_delimiter,
                self.destination_format
            ),
            self.gcp_conn_id
        )
        return bigquery_job_operator
