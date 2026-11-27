#!/usr/bin/env bash
# Regenerate ci/requirements-ci.txt from ci/requirements-ci.in.
# Uses the official Airflow constraints, minus the packages whose versions
# the CI environment pins itself. Requires uv.
set -euo pipefail
cd "$(dirname "$0")"

AIRFLOW_VERSION=2.3.4
PYTHON_VERSION=3.9
CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
PINNED_BY_DEBUSSY='^(apache-airflow-providers-google|facebook-business|paramiko|pg8000)=='

constraints="$(mktemp)"
trap 'rm -f "$constraints"' EXIT
curl -fsSL "$CONSTRAINTS_URL" | grep -Ev "$PINNED_BY_DEBUSSY" > "$constraints"

uv pip compile requirements-ci.in \
  --python-version "$PYTHON_VERSION" \
  --constraint "$constraints" \
  --no-header --no-annotate \
  --output-file requirements-ci.txt
