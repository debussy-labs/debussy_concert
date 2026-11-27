"""Airflow backend. ``load_dags`` is imported on first use because it pulls in
every pipeline, which in turn import this package."""


def __getattr__(name):
    if name in ("load_dags", "build_dag"):
        from debussy.backends.airflow import loader

        return getattr(loader, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
