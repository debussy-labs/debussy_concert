from typing import Protocol

from airflow_concert.motif.motif_base import PMotif


class PExportDataToStorageMotif(PMotif, Protocol):
    destination_storage_uri: str


class PMergeLandingToRawMotif(PMotif, Protocol):
    main_table_uri: str
    delta_table_uri: str


class PCreateExternalTableMotif(PMotif, Protocol):
    source_bucket_uri_prefix: str
    destination_project_dataset_table: str


class PRawToTrustedMotif(PMotif, Protocol):
    pass
