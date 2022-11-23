from debussy_concert.utils.composition import airflow_easy_setup

debussy_composition = airflow_easy_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/reverse_etl/bigquery_to_mysql/composition.yaml",
    composition_type="reverse_etl_bigquery_to_mysql",
)
reverse_etl_movement_fn = (
    debussy_composition.bigquery_to_storage_reverse_etl_to_rdbms_movement_builder
)
dag = debussy_composition.play(reverse_etl_movement_fn)
