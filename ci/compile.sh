#!/usr/bin/env bash
# Regenerate ci/requirements-ci-<python>.txt from ci/requirements-ci.in, one
# file per Python version tested in CI, using the official Airflow
# constraints. Requires uv.
set -euo pipefail
cd "$(dirname "$0")"

AIRFLOW_VERSION=3.4.0
PYTHON_VERSIONS=(3.11 3.13)

# Lowest patch release to resolve for: gcloud-aio-auth requires 3.11.3+.
declare -A MIN_PATCH=([3.11]=3.11.3 [3.13]=3.13.0)

for python_version in "${PYTHON_VERSIONS[@]}"; do
  uv pip compile requirements-ci.in \
    --python-version "${MIN_PATCH[$python_version]}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${python_version}.txt" \
    --no-header --no-annotate \
    --output-file "requirements-ci-${python_version}.txt"
done
