from debussy_concert.utils.easy_setup import setup_this_composition_for_airflow

dags_folder_rel_path_env_file = "examples/environment.yaml"
dags_folder_rel_path_composition_file = (
    "examples/data_ingestion/bigquery_ingestion_incr/composition.yaml"
)
debussy_composition = setup_this_composition_for_airflow(
    dags_folder_rel_path_composition_file,
    dags_folder_rel_path_env_file,
    'ingestion_bigquery'
)
dag = debussy_composition.auto_play()