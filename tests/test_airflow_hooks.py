"""Storage hooks: creating one must not need its connection."""

import pytest

pytest.importorskip("airflow")

from airflow.sdk.exceptions import AirflowNotFoundException  # noqa: E402

from debussy.backends.airflow.hooks import GCSHook  # noqa: E402


def test_gcs_hook_reads_its_connection_on_first_use(monkeypatch):
    monkeypatch.delenv("AIRFLOW_CONN_NO_SUCH_CONNECTION", raising=False)
    hook = GCSHook(gcp_conn_id="no_such_connection")  # parse time: no lookup
    assert hook.gcp_conn_id == "no_such_connection"
    with pytest.raises(AirflowNotFoundException, match="no_such_connection"):
        hook.exists_file("gs://bucket/path/file.csv")


def test_gcs_hook_delegates_to_the_airflow_hook(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_SOME_GCP", "google-cloud-platform://")
    hook = GCSHook(gcp_conn_id="some_gcp")
    calls = []
    monkeypatch.setattr(
        type(hook.hook), "exists", lambda self, **kwargs: calls.append(kwargs) or True
    )
    assert hook.exists_file("gs://bucket/path/file.csv")
    assert calls == [{"bucket_name": "bucket", "object_name": "path/file.csv"}]
    assert hook.hook.gcp_conn_id == "some_gcp"
