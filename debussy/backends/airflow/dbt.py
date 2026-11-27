"""dbt operators for Airflow."""

import json
import os
import signal
import subprocess
from pathlib import Path

from airflow.sdk import BaseHook, BaseOperator
from airflow.sdk.exceptions import AirflowException
from yaml import safe_dump
from yaml_env_var_parser import load as yaml_load


class DbtRunOperator(BaseOperator):
    """Run ``dbt run`` in a subprocess.

    Replaces the operator from the airflow-dbt package (unmaintained, does not
    run on Airflow 3), keeping its arguments.
    """

    ui_color = "#d6522a"
    template_fields = ("vars",)

    def __init__(
        self,
        profiles_dir=None,
        target=None,
        dir=".",
        vars=None,
        models=None,
        exclude=None,
        select=None,
        dbt_bin="dbt",
        verbose=True,
        warn_error=False,
        full_refresh=False,
        data=False,
        schema=False,
        output_encoding="utf-8",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.profiles_dir = profiles_dir
        self.target = target
        self.dir = dir
        self.vars = vars
        self.models = models
        self.exclude = exclude
        self.select = select
        self.dbt_bin = dbt_bin
        self.verbose = verbose
        self.warn_error = warn_error
        self.full_refresh = full_refresh
        self.data = data
        self.schema = schema
        self.output_encoding = output_encoding
        self._process = None

    def dbt_command(self, command="run"):
        cmd = [self.dbt_bin]
        if self.warn_error:
            cmd.append("--warn-error")
        cmd.append(command)
        options = [
            ("--profiles-dir", self.profiles_dir),
            ("--target", self.target),
            ("--vars", json.dumps(self.vars) if self.vars is not None else None),
            ("--models", self.models),
            ("--exclude", self.exclude),
            ("--select", self.select),
        ]
        for flag, value in options:
            if value is not None:
                cmd.extend([flag, value])
        for flag, enabled in (
            ("--data", self.data),
            ("--schema", self.schema),
            ("--full-refresh", self.full_refresh),
        ):
            if enabled:
                cmd.append(flag)
        return cmd

    def execute(self, context):
        cmd = self.dbt_command()
        if self.verbose:
            self.log.info(" ".join(cmd))
        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.dir,
            close_fds=True,
            start_new_session=True,
        )
        for line in iter(self._process.stdout.readline, b""):
            self.log.info(line.decode(self.output_encoding).rstrip())
        self._process.wait()
        self.log.info("Command exited with return code %s", self._process.returncode)
        if self._process.returncode:
            raise AirflowException("dbt command failed")

    def on_kill(self):
        if self._process is not None:
            self.log.info("Sending SIGTERM signal to dbt command")
            os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)


class DebussyDbtRunOperator(DbtRunOperator):
    file_name = Path("profiles.yml")
    tmp_folder_template = "/tmp/{project_name}_dbt/"

    def __init__(
        self,
        project_name,
        connection_id,
        profiles_dir=None,
        target=None,
        **kwargs,
    ):
        self.project_name = project_name
        super().__init__(profiles_dir=profiles_dir, target=target, **kwargs)
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
                    # Google provider 10+ stores the key without the prefix.
                    keyfile_extra = conn.extra_dejson.get(
                        "keyfile_dict"
                    ) or conn.extra_dejson.get(
                        "extra__google_cloud_platform__keyfile_dict"
                    )
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
