from airflow.utils.task_group import TaskGroup

from airflow_dbt.operators.dbt_operator import DbtRunOperator
from debussy_concert.core.motif.motif_base import MotifBase
from debussy_concert.pipeline.transform.config.movement_parameters.dbt import DbtParameters


import json
import os
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from pathlib import Path
from yaml_env_var_parser import load as yaml_load
from yaml import safe_dump


class ParseDbtProfilesFile(BaseOperator):
    file_name = Path('profiles.yml')

    def __init__(
            self,
            origin_profiles_dir,
            destination_profiles_dir,
            connection_id=None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.origin_profiles_dir = Path(origin_profiles_dir)
        self.destination_profiles_dir = Path(destination_profiles_dir)
        self.connection_id = connection_id

    def fill_bigquery_credentials(self, yaml_content: dict):
        if not self.connection_id:
            return

        for key, value in yaml_content.items():
            if key == 'config':
                continue
            outputs: dict = value['outputs']
            for target in outputs.values():
                if target['method'].lower() == 'service-account-json':
                    conn = BaseHook.get_connection(self.connection_id)
                    keyfile_extra = conn.extra_dejson['extra__google_cloud_platform__keyfile_dict']
                    target['keyfile_json'] = json.loads(keyfile_extra)

    def execute(self, context):
        origin_file_path = self.origin_profiles_dir / self.file_name
        destination_file_path = self.destination_profiles_dir / self.file_name

        with open(origin_file_path, 'r') as handle:
            content = yaml_load(handle)
        self.fill_bigquery_credentials(content)

        destination_file_path.parent.mkdir(exist_ok=True, parents=True)
        with open(destination_file_path, 'w+') as handle:
            safe_dump(content, handle)


class DebussyDbtRunOperator(DbtRunOperator):
    def __init__(self, project_name, profiles_dir=None, target=None, *args, **kwargs):
        self.project_name = project_name
        super().__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)

    def shallow_stringfy_dict(self, dict_: dict):
        new_dict = {}
        for key, value in dict_.items():
            new_dict[key] = str(value)
        return new_dict

    def stringfy_context(self, context):
        new_dict = self.shallow_stringfy_dict(context)
        return new_dict

    def config_dbt_path_env_vars(self):
        """
        The precedence order is: CLI flag > env var > dbt_project.yml
        """
        base_path = f"/tmp/{self.project_name}_dbt"
        log_path = base_path + "/logs"
        target_path = base_path + "/target"
        packages_path = base_path + "/dbt_packages"
        os.environ['DBT_LOG_PATH'] = log_path
        os.environ['DBT_TARGET_PATH'] = target_path
        # FIXME: this env var is not working and there is no doc on dbt on how to set this besides dbt_project.yaml
        # so this is not done automatic and this env var should be read on dbt_project.yaml
        os.environ['DBT_PACKAGES_INSTALL_PATH'] = packages_path

    def execute(self, context):
        self.config_dbt_path_env_vars()
        new_context = self.stringfy_context(context)
        if self.vars is None:
            self.vars = new_context
        else:
            self.vars.update(new_context)
        super().execute(context)


class DbtRunMotif(MotifBase):
    destination_profile_dir_template = '/tmp/{composition_name}_dbt/'

    def __init__(
            self,
            dbt_run_parameters: DbtParameters,
            movement_name,
            name=None
    ):
        self.dbt_run_parameters = dbt_run_parameters
        self.movement_name = movement_name
        super().__init__(name=name)

    def setup(self):
        ...

    def build(self, workflow_dag, phrase_group):
        task_group = TaskGroup(group_id=self.name, dag=workflow_dag, parent_group=phrase_group)
        final_profile_dir = None

        if self.dbt_run_parameters.profiles_dir:
            final_profile_dir = self.destination_profile_dir_template.format(
                composition_name=self.config.name
            )
            parse_profiles = ParseDbtProfilesFile(
                task_id="parse_profiles_file",
                dag=workflow_dag,
                task_group=task_group,
                origin_profiles_dir=self.dbt_run_parameters.profiles_dir,
                destination_profiles_dir=final_profile_dir,
                connection_id=self.config.environment.data_lakehouse_connection_id
            )

        run_dbt = DebussyDbtRunOperator(
            task_id="dbt_run",
            dag=workflow_dag,
            project_name=self.movement_name,
            task_group=task_group,
            dir=self.dbt_run_parameters.dir,
            profiles_dir=final_profile_dir,
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
            schema=self.dbt_run_parameters.schema
        )

        if self.dbt_run_parameters.profiles_dir:
            parse_profiles >> run_dbt

        return task_group
