"""Keys of a composition YAML file that tell a loader how to build the pipeline.

They are read by the loader (see ``debussy.backends.airflow.load_dags``), not by
the composition config classes, which drop them when they load the file.
"""

# Name of the composition type, for example ``rdbms_ingestion``.
COMPOSITION_TYPE_KEY = "composition_type"
# Prefix of the ``<PREFIX>_WINDOW_START`` and ``<PREFIX>_WINDOW_END`` environment
# variables that the YAML may reference as ``${<PREFIX>_WINDOW_START}``.
ENV_PREFIX_KEY = "os_env_prefix"

LOADER_KEYS = (COMPOSITION_TYPE_KEY, ENV_PREFIX_KEY)


def drop_loader_keys(config: dict) -> dict:
    """Remove the loader keys from a loaded composition YAML, in place."""
    for key in LOADER_KEYS:
        config.pop(key, None)
    return config
