"""Airflow DAGs-folder entry point for the YAML examples.

Every YAML file below ``examples/`` with a ``composition_type`` key becomes a DAG.
"""

from debussy.backends.airflow import load_dags

load_dags(
    globals(),
    pipelines_dir="examples",
    environment_file="examples/environment.yaml",
)
