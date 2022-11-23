from debussy_concert.utils.composition import airflow_easy_setup

debussy_composition = airflow_easy_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_ingestion/bigquery_ingestion_incr/composition.yaml",
    composition_type="bigquery_ingestion",
)

dag = debussy_composition.auto_play()
