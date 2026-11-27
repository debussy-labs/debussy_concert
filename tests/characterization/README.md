# Characterization tests

These tests pin down what Debussy generates today, so refactoring cannot
silently change it.

`test_example_dags.py` runs every DAG file under `examples/` with
`AirflowService`, the same way the Airflow scheduler parses it, and compares
the result with a JSON snapshot in `snapshots/`. A snapshot records:

- DAG arguments (schedule, dates, tags, default args)
- task groups and their parents
- each task's operator class, task group, trigger rule and upstream tasks
- the raw (unrendered) value of every templated field, and the Python callable
  of Python operators

Values that are not plain data are reduced to a stable name, the repository
path is replaced with `<DAGS_FOLDER>`, and dummy Airflow connections are set
through environment variables, so the snapshots are the same on every machine.

## When a snapshot test fails

A failure means the generated DAG changed. If the change is intended,
regenerate the snapshots and review the diff in the pull request:

```
DEBUSSY_UPDATE_SNAPSHOTS=1 pytest tests/characterization
git diff tests/characterization/snapshots
```

## Known broken examples

Examples that do not build on the current code are listed in `KNOWN_BROKEN`
in `test_example_dags.py`. They run as strict expected failures: once one is
fixed, the test fails until its entry is removed and a snapshot is created.

## Environment

The snapshots describe the DAGs built on Airflow 3.4, and must be identical on
every Python version tested in CI (3.11 and 3.13). The CI environments are
pinned in `ci/requirements-ci-<python>.txt`; regenerate them with
`ci/compile.sh` after changing `ci/requirements-ci.in`.
