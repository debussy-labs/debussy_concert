"""Storage and database hooks used by the reverse ETL pipelines.

Ported from debussy_airflow 0.0.7, which is discontinued and does not run on
Airflow 3.
"""

import io
import os
import stat
from contextlib import contextmanager
from functools import cached_property
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, Optional, Sequence, Union
from uuid import uuid4

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook as AirflowGCSHook
from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url
from airflow.sdk import BaseHook

if TYPE_CHECKING:
    import paramiko
    from pandas import DataFrame


def parse_blob_list_to_ls_behavior(full_path_blobs, prefix):
    """Return the names directly under ``prefix``, like ``ls`` would."""
    relative_path_blobs = [
        full_path_blob.replace(prefix, "", 1) for full_path_blob in full_path_blobs
    ]
    return list(
        {relative_path_blob.split("/")[0] for relative_path_blob in relative_path_blobs}
    )


class StorageHookInterface(BaseHook, ABC):
    @abstractmethod
    def upload_file(self, remote_file_uri, local_file_uri, **kwargs):
        pass

    @abstractmethod
    def upload_string(self, remote_file_uri, data_string, **kwargs):
        pass

    @abstractmethod
    def download_file(self, remote_file_uri, local_file_uri, **kwargs):
        pass

    @abstractmethod
    def download_string(self, remote_file_uri, **kwargs):
        pass

    @abstractmethod
    def exists_file(self, file_path, **kwargs):
        pass

    @abstractmethod
    def list_dir(self, remote_uri):
        pass

    @staticmethod
    def basename(path_uri):
        return os.path.basename(path_uri)


class GCSHook(StorageHookInterface):
    """Google Cloud Storage on top of Airflow's ``GCSHook``.

    Airflow's hook reads its connection when it is created. The pipelines
    create their hooks while the DAG file is parsed, so the Airflow hook is
    created on first use instead: a missing connection must fail the task that
    needs it, not the parsing of every DAG in the file.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> AirflowGCSHook:
        return AirflowGCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def get_conn(self):
        return self.hook.get_conn()

    def upload_file(
        self, remote_file_uri, local_file_uri, mime_type="application/octet-stream"
    ):
        self.log.info("Uploading file from %s to %s", local_file_uri, remote_file_uri)
        bucket, object_path = _parse_gcs_url(remote_file_uri)
        return self.hook.upload(
            bucket_name=bucket,
            object_name=object_path,
            filename=local_file_uri,
            mime_type=mime_type,
        )

    def upload_string(self, remote_file_uri, data_string, mime_type="text/plain"):
        bucket, object_path = _parse_gcs_url(remote_file_uri)
        blob = self.get_conn().bucket(bucket).blob(blob_name=object_path)
        blob.upload_from_string(data=data_string, content_type=mime_type)

    def download_file(self, remote_file_uri, local_file_uri):
        bucket, object_path = _parse_gcs_url(remote_file_uri)
        self.hook.download(bucket, object_path, local_file_uri)

    def download_string(self, remote_file_uri, encoding="utf-8"):
        bucket, object_path = _parse_gcs_url(remote_file_uri)
        return self.hook.download(bucket, object_path).decode(encoding)

    def exists_file(self, file_path):
        bucket, object_path = _parse_gcs_url(file_path)
        return self.hook.exists(bucket_name=bucket, object_name=object_path)

    def list_dir(self, remote_uri):
        if not remote_uri.endswith("/"):
            remote_uri = remote_uri + "/"
        bucket, object_path = _parse_gcs_url(remote_uri)
        full_path_blobs = self.hook.list(bucket_name=bucket, prefix=object_path)
        return parse_blob_list_to_ls_behavior(full_path_blobs, prefix=object_path)


class SFTPHook(StorageHookInterface):
    """SFTP storage on top of paramiko.

    Connection extras: ``ignore_hostkey_verification``, ``private_key`` and
    ``private_key_pass``.
    """

    def __init__(self, sftp_conn_id: str, temp_folder: str = "/tmp"):
        super().__init__()
        self.sftp_conn_id = sftp_conn_id
        self.temp_folder = temp_folder

    def get_conn(self) -> "paramiko.SSHClient":
        import paramiko

        if self.sftp_conn_id is None:
            raise ValueError("sftp_conn_id is None")
        conn = self.get_connection(self.sftp_conn_id)
        extra = conn.extra_dejson

        ssh = paramiko.SSHClient()
        if extra.get("ignore_hostkey_verification"):
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        else:
            ssh.load_system_host_keys()
        private_key = None
        if "private_key" in extra:
            private_key = paramiko.RSAKey.from_private_key(
                io.StringIO(extra["private_key"]),
                password=extra.get("private_key_pass"),
            )
        ssh.connect(
            hostname=conn.host,
            port=conn.port or 22,
            username=conn.login,
            password=conn.password,
            pkey=private_key,
        )
        return ssh

    @contextmanager
    def _sftp(self):
        ssh = self.get_conn()
        try:
            with ssh.open_sftp() as sftp:
                yield sftp
        finally:
            ssh.close()

    def upload_file(self, remote_file_uri, local_file_uri):
        self.log.info("Uploading file from %s to %s", local_file_uri, remote_file_uri)
        with self._sftp() as sftp:
            # remote_file_uri is the destination folder, as in debussy_airflow.
            if remote_file_uri:
                sftp.chdir(remote_file_uri)
            sftp.put(local_file_uri, os.path.basename(local_file_uri))

    def upload_string(self, remote_file_uri, data_string):
        local_filename = os.path.join(self.temp_folder, f"{uuid4()}")
        with open(local_filename, "w") as f:
            f.write(data_string)
        self.upload_file(remote_file_uri=remote_file_uri, local_file_uri=local_filename)

    def list_dir(self, remote_uri):
        with self._sftp() as sftp:
            return [
                attr.filename
                for attr in sftp.listdir_attr(remote_uri)
                if stat.S_ISREG(attr.st_mode)
            ]

    def download_file(self, remote_file_uri, local_file_uri):
        self.log.info("Downloading file from %s to %s", remote_file_uri, local_file_uri)
        with self._sftp() as sftp:
            sftp.get(remote_file_uri, local_file_uri)

    def download_string(self, remote_file_uri):
        local_filename = os.path.join(self.temp_folder, f"{uuid4()}")
        self.download_file(
            remote_file_uri=remote_file_uri, local_file_uri=local_filename
        )
        with open(local_filename) as f:
            return f.read()

    def exists_file(self, file_path):
        raise NotImplementedError()


class DbApiHookInterface(DbApiHook):
    @abstractmethod
    def query_run(self, sql: str, autocommit: bool):
        pass

    @abstractmethod
    def build_upsert_query(self, table_name: str, dataset_table: "DataFrame"):
        pass


class MySqlConnectorHook(DbApiHookInterface):
    conn_name_attr = "rdbms_conn_id"
    default_conn_name = "rdbms_conn_id"

    def _get_conn_config_mysql_client(self, conn) -> Dict:
        return {
            "user": conn.login,
            "passwd": conn.password or "",
            "host": conn.host or "localhost",
            "db": conn.schema,
            "port": int(conn.port) if conn.port else 3306,
        }

    def get_conn(self):
        import MySQLdb

        if self.rdbms_conn_id is None:
            raise ValueError("rdbms_conn_id is None")
        conn = self.get_connection(self.rdbms_conn_id)
        return MySQLdb.connect(**self._get_conn_config_mysql_client(conn))

    def build_upsert_query(self, table_name: str, dataset_table: "DataFrame"):
        if dataset_table.empty:
            return None

        escaped_columns = ",".join(f"`{c}`" for c in dataset_table.columns)
        records = ",".join(
            str(record).replace("[", "(").replace("]", ")")
            for record in dataset_table.values.tolist()
        )
        return f"REPLACE INTO {table_name}({escaped_columns}) VALUES {records};"

    def query_run(self, sql: str, autocommit: bool):
        self.run(sql=sql, autocommit=autocommit)
