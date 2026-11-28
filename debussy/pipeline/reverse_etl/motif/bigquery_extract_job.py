from typing import List, Optional, Union

from debussy.core.motif.motif_base import MotifBase
from debussy.core.service.lakehouse.bigquery_job import BigQueryJobMixin
from debussy.core.tasks import TaskSpec


class BigQueryExtractJobMotif(MotifBase, BigQueryJobMixin):
    group_tasks = False

    def __init__(
        self,
        field_delimiter: Optional[str] = ",",
        destination_format: Optional[str] = "CSV",
        print_header: Optional[bool] = True,
        gcp_conn_id="google_cloud_default",
        name=None,
    ):
        super().__init__(name=name)
        self.field_delimiter = field_delimiter
        self.destination_format = destination_format
        self.print_header = print_header
        self.gcp_conn_id = gcp_conn_id

    def setup(self, source_table_uri: str, destination_uris: Union[List[str], str]):
        self.source_table_uri = source_table_uri
        self.destination_uris = destination_uris
        return self

    def tasks(self):
        if self.source_table_uri is None:
            raise ValueError("source_table_uri cant be none. initialize on setup")
        if self.destination_uris is None:
            raise ValueError("destination_uris cant be none. initialize on setup")
        return [
            TaskSpec(
                name=self.name,
                action="bigquery.insert_job",
                params={
                    "configuration": self.extract_configuration(
                        self.source_table_uri,
                        self.destination_uris,
                        self.field_delimiter,
                        self.destination_format,
                        self.print_header,
                    ),
                    "gcp_conn_id": self.gcp_conn_id,
                },
                templated={"configuration"},
            )
        ]
