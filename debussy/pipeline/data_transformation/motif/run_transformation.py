from airflow.sdk import TaskGroup

from debussy.backends.airflow.dbt import DebussyDbtRunOperator
from debussy.core.motif.motif_base import MotifBase
from debussy.pipeline.data_transformation.config.movement_parameters.dbt import (
    DbtParameters,
)


class DbtRunMotif(MotifBase):
    def __init__(self, dbt_run_parameters: DbtParameters, movement_name, name=None):
        self.dbt_run_parameters = dbt_run_parameters
        self.movement_name = movement_name
        super().__init__(name=name)

    def setup(self): ...

    def build(self, workflow_dag, phrase_group):

        run_dbt = DebussyDbtRunOperator(
            task_id="dbt_run",
            dag=workflow_dag,
            task_group=phrase_group,
            project_name=self.movement_name,
            connection_id=self.config.environment.data_lakehouse_connection_id,
            dir=self.dbt_run_parameters.dir,
            profiles_dir=self.dbt_run_parameters.profiles_dir,
            target=self.dbt_run_parameters.target,
            vars=self.dbt_run_parameters.vars,
            models=self.dbt_run_parameters.models,
            exclude=self.dbt_run_parameters.exclude,
            select=self.dbt_run_parameters.select,
            dbt_bin=self.dbt_run_parameters.dbt_bin,
            verbose=self.dbt_run_parameters.verbose,
            warn_error=self.dbt_run_parameters.warn_error,
            full_refresh=self.dbt_run_parameters.full_refresh,
            data=self.dbt_run_parameters.data,
            schema=self.dbt_run_parameters.schema,
        )

        return run_dbt
