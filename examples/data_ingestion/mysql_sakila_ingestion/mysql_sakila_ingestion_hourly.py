from debussy_concert.utils.composition import airflow_easy_setup
from debussy_concert.pipeline.data_ingestion.composition.rdbms_ingestion import \
    RdbmsIngestionComposition
from debussy_concert.pipeline.data_ingestion.config.rdbms_data_ingestion import \
    ConfigRdbmsDataIngestion

debussy_composition = airflow_easy_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_ingestion/mysql_sakila_ingestion/composition_hourly.yaml",
    CompositionCls=RdbmsIngestionComposition,
    CompositionConfigCls=ConfigRdbmsDataIngestion,
    os_env_prefix='MYSQL_SAKILA_HOURLY'
)

dag = debussy_composition.auto_play()
