from datetime import datetime
from airflow.configuration import conf
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from debussy_concert.core.service.injection import inject_dependencies
from debussy_concert.core.service.lakehouse.google_cloud import BigQueryTable
from debussy_concert.data_ingestion.motif.create_update_table_schema import CreateOrUpdateBigQueryTableMotif

dag = DAG(dag_id='create_table_motif_example', schedule_interval=None, start_date=datetime(year=2022, month=1, day=1))
phrase_group = TaskGroup(group_id='PhraseGroup', dag=dag)
dags_folder = conf.get('core', 'dags_folder')

schema_file = f'{dags_folder}/examples/create_table_motif/table_schema.yaml'
table = BigQueryTable.load_from_file(schema_file)

inject_dependencies(workflow_service=None, config_composition=None)
motif = CreateOrUpdateBigQueryTableMotif(table=table)
motif.setup(destination_table_uri='modular-aileron-191222.temp.create_table_motif_test')
task = motif.build(
    workflow_dag=dag,
    phrase_group=phrase_group
)
