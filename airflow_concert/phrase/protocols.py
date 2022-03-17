from typing import Protocol

from airflow_concert.motif.motif_base import PMotif


class PExportDataToStorageMotif(PMotif, Protocol):
    destination_storage_uri: str

    def setup(self, destination_storage_uri: str):
        pass


class PMergeTableMotif(PMotif, Protocol):
    main_table_uri: str
    delta_table_uri: str

    def setup(
        self,
        main_table_uri: str,
        delta_table_uri: str,
    ):
        pass


class PExecuteQueryMotif(PMotif, Protocol):
    sql_query: str

    def setup(self, sql_query: str):
        pass


class PCreateExternalTableMotif(PMotif, Protocol):
    source_storage_uri_prefix: str
    destination_table_uri: str

    def setup(
        self,
        source_bucket_uri_prefix: str,
        destination_project_dataset_table: str
    ):
        pass


class PRawToTrustedMotif(PMotif, Protocol):
    pass
