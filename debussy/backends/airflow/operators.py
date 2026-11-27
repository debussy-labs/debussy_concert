"""Operators used by the reverse ETL pipelines.

Ported from debussy_airflow 0.0.7, which is discontinued and does not run on
Airflow 3.
"""

import os
from typing import Callable, Optional
from uuid import uuid4

from airflow.sdk import BaseOperator

from debussy.backends.airflow.hooks import DbApiHookInterface, StorageHookInterface


class StorageToStorageOperator(BaseOperator):
    """Download data from origin and upload it to destination."""

    template_fields = ("origin_file_uri", "destination_file_uri")

    def __init__(
        self,
        origin_storage_hook: StorageHookInterface,
        origin_file_uri,
        destination_storage_hook: StorageHookInterface,
        destination_file_uri,
        temp_folder="/tmp",
        is_dir=False,
        file_filter_fn: Optional[Callable[[dict, str], bool]] = None,
        file_transformer_fn: Optional[Callable[[str], str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.origin_storage_hook = origin_storage_hook
        self.destination_storage_hook = destination_storage_hook
        self.origin_file_uri = origin_file_uri
        self.destination_file_uri = destination_file_uri
        self.temp_folder = temp_folder
        self.is_dir = is_dir
        self.file_filter_fn = file_filter_fn or (lambda context, file_name: True)
        self.file_transformer_fn = file_transformer_fn or (
            lambda tmp_file_uri: tmp_file_uri
        )
        self._temp_folder = temp_folder

    def file_to_file(self, origin_file_uri, destination_file_uri):
        self.log.info("Retrieving data from %s", origin_file_uri)
        tmp_filepath = os.path.join(
            self._temp_folder, self.origin_storage_hook.basename(origin_file_uri)
        )
        self.origin_storage_hook.download_file(origin_file_uri, tmp_filepath)
        transformed_file_uri = self.file_transformer_fn(tmp_filepath)
        self.log.info("Uploading data to %s", destination_file_uri)
        self.destination_storage_hook.upload_file(
            destination_file_uri, transformed_file_uri
        )

    def folder_to_folder(self, context):
        for file_name in self.origin_storage_hook.list_dir(self.origin_file_uri):
            if self.file_filter_fn(context, file_name):
                origin = os.path.join(self.origin_file_uri, file_name)
                destination = os.path.join(self.destination_file_uri, file_name)
                self.file_to_file(origin, destination)

    def execute(self, context):
        uri_to_name = self.origin_file_uri.replace("/", "__").replace("\\", "__")
        self._temp_folder = os.path.join(self.temp_folder, f"{uri_to_name}-{uuid4()}")
        os.mkdir(self._temp_folder)
        if self.is_dir:
            self.folder_to_folder(context)
        else:
            self.file_to_file(self.origin_file_uri, self.destination_file_uri)


class StorageToRdbmsOperator(BaseOperator):
    """Download a CSV file from storage and upsert its rows into a table."""

    template_fields = ("storage_file_uri", "table_name")

    def __init__(
        self,
        dbapi_hook: DbApiHookInterface,
        storage_hook: StorageHookInterface,
        table_name,
        storage_file_uri,
        temp_folder="/tmp",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dbapi_hook = dbapi_hook
        self.storage_hook = storage_hook
        self.storage_file_uri = storage_file_uri
        self.table_name = table_name
        self.temp_folder = temp_folder

    def execute(self, context):
        tmp_filepath = os.path.join(
            self.temp_folder, self.storage_hook.basename(self.storage_file_uri)
        )
        self.storage_hook.download_file(self.storage_file_uri, tmp_filepath)

        import pandas as pd

        self.log.info("Reading %s", tmp_filepath)
        dataset_table = pd.read_csv(
            tmp_filepath,
            encoding="utf-8",
            delimiter=",",
            low_memory=False,
            keep_default_na=False,
            na_values=None,
        )
        query = self.dbapi_hook.build_upsert_query(
            table_name=self.table_name, dataset_table=dataset_table
        )
        return self.dbapi_hook.query_run(sql=query, autocommit=True)
