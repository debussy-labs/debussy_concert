from debussy_concert.core.config.utils import composition_setup
from debussy_concert.pipeline.data_ingestion.composition.bigquery_ingestion import \
    BigQueryIngestionComposition
from debussy_concert.pipeline.data_ingestion.config.bigquery_data_ingestion import \
    ConfigBigQueryDataIngestion

debussy_composition = composition_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_ingestion/bigquery_ingestion_incr/composition.yaml",
    CompositionCls=BigQueryIngestionComposition,
    CompositionConfigCls=ConfigBigQueryDataIngestion,
)

dag = debussy_composition.auto_play()
