from dataclasses import dataclass
from typing import Optional
from debussy_concert.core.config.movement_parameters.base import MovementParametersBase


@dataclass(frozen=True)
class DbtMovementParameters(MovementParametersBase):
    """
    :param profiles_dir: If set, passed as the `--profiles-dir` argument to the `dbt` command
    :type profiles_dir: str
    :param target: If set, passed as the `--target` argument to the `dbt` command
    :type target: str
    :param dir: The directory to run the CLI in
    :type dir: str
    :param vars: If set, passed as the `--vars` argument to the `dbt` command
    :type vars: dict
    :param full_refresh: If `True`, will fully-refresh incremental models.
    :type full_refresh: bool
    :param models: If set, passed as the `--models` argument to the `dbt` command
    :type models: str
    :param warn_error: If `True`, treat warnings as errors.
    :type warn_error: bool
    :param exclude: If set, passed as the `--exclude` argument to the `dbt` command
    :type exclude: str
    :param select: If set, passed as the `--select` argument to the `dbt` command
    :type select: str
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
    :type dbt_bin: str
    :param verbose: The operator will log verbosely to the Airflow logs
    :type verbose: bool
    :param data: ???
    :type data: bool
    :param schema: ???
    :type schema: bool
    """
    profiles_dir: Optional[str] = None
    target: Optional[str] = None
    dir: Optional[str] = '.'
    vars: Optional[dict] = None
    models: Optional[str] = None
    exclude: Optional[str] = None
    select: Optional[str] = None
    dbt_bin: Optional[str] = 'dbt'
    verbose: Optional[bool] = True
    warn_error: Optional[bool] = False
    full_refresh: Optional[bool] = False
    data: Optional[bool] = False
    schema: Optional[bool] = False
