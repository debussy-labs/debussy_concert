from debussy_concert.utils.composition import airflow_easy_setup

debussy_composition = airflow_easy_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_ingestion/postgresql_sakila_ingestion/composition.yaml",
    composition_type="rdbms_ingestion",
    os_env_prefix="POSTGRESQL_SAKILA",
)

dag = debussy_composition.auto_play()
