from debussy.utils.composition import airflow_easy_setup
from debussy.pipeline.data_ingestion.composition.bigquery_ingestion import (
    BigQueryIngestionComposition,
)
from debussy.pipeline.data_ingestion.config.bigquery_data_ingestion import (
    ConfigBigQueryDataIngestion,
)

debussy_composition = airflow_easy_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_ingestion/bigquery_ingestion_policy_tags/composition.yaml",
    composition_cls=BigQueryIngestionComposition,
    composition_config_cls=ConfigBigQueryDataIngestion,
)

dag = debussy_composition.auto_play()
