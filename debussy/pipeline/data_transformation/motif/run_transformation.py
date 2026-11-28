from debussy.core.motif.motif_base import MotifBase
from debussy.core.tasks import TaskSpec
from debussy.pipeline.data_transformation.config.movement_parameters.dbt import (
    DbtParameters,
)


class DbtRunMotif(MotifBase):
    group_tasks = False

    def __init__(self, dbt_run_parameters: DbtParameters, movement_name, name=None):
        self.dbt_run_parameters = dbt_run_parameters
        self.movement_name = movement_name
        super().__init__(name=name)

    def setup(self): ...

    def tasks(self):
        parameters = self.dbt_run_parameters
        return [
            TaskSpec(
                name="dbt_run",
                action="dbt.run",
                params={
                    "project_name": self.movement_name,
                    "connection_id": self.config.environment.data_lakehouse_connection_id,
                    "dir": parameters.dir,
                    "profiles_dir": parameters.profiles_dir,
                    "target": parameters.target,
                    "vars": parameters.vars,
                    "models": parameters.models,
                    "exclude": parameters.exclude,
                    "select": parameters.select,
                    "dbt_bin": parameters.dbt_bin,
                    "verbose": parameters.verbose,
                    "warn_error": parameters.warn_error,
                    "full_refresh": parameters.full_refresh,
                    "data": parameters.data,
                    "schema": parameters.schema,
                },
                templated={"vars"},
            )
        ]
