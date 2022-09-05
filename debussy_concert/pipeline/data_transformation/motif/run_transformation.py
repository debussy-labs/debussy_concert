from airflow.utils.task_group import TaskGroup

from airflow_dbt.operators.dbt_operator import DbtRunOperator
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.pipeline.data_transformation.config.movement_parameters.dbt import (
    DbtParameters,
)


import json
import os
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from pathlib import Path
from yaml_env_var_parser import load as yaml_load
from yaml import safe_dump


class DebussyDbtRunOperator(DbtRunOperator):
    file_name = Path("profiles.yml")
    tmp_folder_template = "/tmp/{project_name}_dbt/"

    def __init__(
        self,
        project_name,
        connection_id,
        profiles_dir=None,
        target=None,
        *args,
        **kwargs
    ):
        self.project_name = project_name
        super().__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)
        self._profiles_dir = Path(profiles_dir) if profiles_dir else None
        self.connection_id = connection_id
        self.base_path = Path(
            self.tmp_folder_template.format(project_name=self.project_name)
        )

    def shallow_stringfy_dict(self, dict_: dict):
        new_dict = {}
        for key, value in dict_.items():
            new_dict[key] = str(value)
        return new_dict

    def stringfy_context(self, context):
        new_dict = self.shallow_stringfy_dict(context)
        return new_dict

    def fill_bigquery_credentials(self, yaml_content: dict):
        if not self.connection_id:
            return

        for key, value in yaml_content.items():
            if key == "config":
                continue
            outputs: dict = value["outputs"]
            for target in outputs.values():
                if target["method"].lower() == "service-account-json":
                    conn = BaseHook.get_connection(self.connection_id)
                    keyfile_extra = conn.extra_dejson[
                        "extra__google_cloud_platform__keyfile_dict"
                    ]
                    target["keyfile_json"] = json.loads(keyfile_extra)

    def read_profiles_file(self, path: Path):
        with open(path, "r") as handle:
            content = yaml_load(handle)
        return content

    def create_profiles_file(self, path: Path, content: dict):
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, "w+") as handle:
            safe_dump(content, handle)

    def update_profiles(self):
        origin_file_path = self._profiles_dir / self.file_name
        destination_file_path = self.base_path / self.file_name
        content = self.read_profiles_file(origin_file_path)
        self.fill_bigquery_credentials(content)
        self.create_profiles_file(destination_file_path, content)
        self.profiles_dir = str(self.base_path)

    def config_dbt_path_env_vars(self):
        """
        The precedence order is: CLI flag > env var > dbt_project.yml
        """

        log_path = self.base_path / "logs"
        target_path = self.base_path / "target"
        packages_path = self.base_path / "dbt_packages"
        os.environ["DBT_LOG_PATH"] = str(log_path)
        os.environ["DBT_TARGET_PATH"] = str(target_path)
        # FIXME: this env var is not working and there is no doc on dbt on how to set this besides dbt_project.yaml
        # so this is not done automatic and this env var should be read on dbt_project.yaml
        os.environ["DBT_PACKAGES_INSTALL_PATH"] = str(packages_path)

    def execute(self, context):
        if self._profiles_dir:
            self.update_profiles()
        self.config_dbt_path_env_vars()
        new_context = self.stringfy_context(context)
        if self.vars is None:
            self.vars = new_context
        else:
            self.vars.update(new_context)
        super().execute(context)


class DbtRunMotif(MotifBase):
    def __init__(self, dbt_run_parameters: DbtParameters, movement_name, name=None):
        self.dbt_run_parameters = dbt_run_parameters
        self.movement_name = movement_name
        super().__init__(name=name)

    def setup(self):
        ...

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
