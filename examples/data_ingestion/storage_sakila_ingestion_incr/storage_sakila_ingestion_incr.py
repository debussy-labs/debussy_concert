import os

from debussy_concert.core.config.utils import composition_setup
from debussy_concert.pipeline.data_ingestion.composition.storage_ingestion import \
    StorageIngestionComposition
from debussy_concert.pipeline.data_ingestion.config.storage_data_ingestion import \
    ConfigStorageDataIngestion

os.environ["STORAGE_INGESTION_WINDOW_START"] = "execution_date.strftime('%Y-%m-%d 00:00:00')"
os.environ["STORAGE_INGESTION_WINDOW_END"] = "next_execution_date.strftime('%Y-%m-%d 00:00:00')"

debussy_composition = composition_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_ingestion/storage_sakila_ingestion_incr/composition.yaml",
    CompositionCls=StorageIngestionComposition,
    CompositionConfigCls=ConfigStorageDataIngestion,
)

dag = debussy_composition.auto_play()
