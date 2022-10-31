from typing import Callable

from debussy_concert.core.motif.motif_base import MotifBase
from debussy_airflow.operators.storage_to_storage_operator import \
    StorageToStorageOperator


def parquet_to_parquet(tmp_file_uri: str) -> str:
    return tmp_file_uri


def csv_to_parquet(tmp_file_uri: str) -> str:
    import pandas as pd
    print(f'Transforming file {tmp_file_uri}')
    table_origin = pd.read_csv(
        tmp_file_uri, keep_default_na=False, na_values=None, dtype=str
    )
    file_suffix = '.csv'
    suffix_len = len(file_suffix)
    if tmp_file_uri.endswith('file_suffix'):
        destination_file_uri = tmp_file_uri[:-suffix_len] + '.parquet'
    else:
        destination_file_uri = tmp_file_uri[:-suffix_len] + '.parquet'
    print(f"Writing to {destination_file_uri}")
    table_origin.to_parquet(destination_file_uri)
    return destination_file_uri


class StorageToRawVaultMotif(MotifBase):
    def __init__(
        self,
        source_storage_hook,
        raw_vault_hook,
        source_file_uri,
        is_dir,
        file_transformer_callable: Callable = None,
        gcs_partition=None,
        name=None
    ):
        super().__init__(name=name)
        self.source_storage_hook = source_storage_hook
        self.raw_vault_hook = raw_vault_hook
        self.source_file_uri = source_file_uri
        self.is_dir = is_dir
        self.file_transformer_callable = file_transformer_callable
        self.gcs_partition = gcs_partition

    def setup(self, destination_storage_uri):
        self.destination_storage_uri = destination_storage_uri
        self.gcs_schema_uri = f"{destination_storage_uri}/" f"{self.gcs_partition}/"

    def build(self, dag, phrase_group):
        storage_to_raw_vault_gcs_task = StorageToStorageOperator(
            task_id="storage_to_raw_vault_gcs",
            origin_storage_hook=self.source_storage_hook,
            origin_file_uri=self.source_file_uri,
            is_dir=self.is_dir,
            destination_storage_hook=self.raw_vault_hook,
            destination_file_uri=self.gcs_schema_uri + "0.parquet",
            # file_filter_fn
            file_transformer_fn=self.file_transformer_callable,
            dag=dag,
            task_group=phrase_group,
        )
        return storage_to_raw_vault_gcs_task
