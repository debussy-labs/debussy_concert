import os

from debussy_concert.core.config.utils import composition_setup
from debussy_concert.pipeline.data_ingestion.composition.rdbms_ingestion import \
    RdbmsIngestionComposition
from debussy_concert.pipeline.data_ingestion.config.rdbms_data_ingestion import \
    ConfigRdbmsDataIngestion

os.environ["MSSQL_SAKILA_WINDOW_START"] = "execution_date.strftime('%Y-%m-%d 00:00:00')"
os.environ["MSSQL_SAKILA_WINDOW_END"] = "next_execution_date.strftime('%Y-%m-%d 00:00:00')"

debussy_composition = composition_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_ingestion/mssql_sakila_ingestion/composition.yaml",
    CompositionCls=RdbmsIngestionComposition,
    CompositionConfigCls=ConfigRdbmsDataIngestion,
)

dag = debussy_composition.auto_play()
