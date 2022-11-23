from debussy_concert.utils.composition import airflow_easy_setup

debussy_composition = airflow_easy_setup(
    rel_path_env_file="examples/environment.yaml",
    rel_path_composition_file="examples/data_transformation/sakila_transformation/composition.yaml",
    composition_type="dbt_transformation",
)
movement_builder = debussy_composition.dbt_transformation_builder
dag = debussy_composition.build(movement_builder)
