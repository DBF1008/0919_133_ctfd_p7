#!/usr/bin/env bash
#
# Manual unit-test runner for CTFd.
#
# Usage:
#   ./test.sh                       run every unit test under tests/
#   ./test.sh tests/test_probes.py  run one or more test files / node ids
#   ./test.sh -k readyz             pass extra arguments straight to pytest
#
# Environment:
#   PYTHON=/path/to/python          override the interpreter (default: python3)
#   PYTEST_ARGS="-x -n auto"        extra pytest arguments
set -euo pipefail

cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3}"

echo "==> Using interpreter: $("${PYTHON}" --version 2>&1)"

if ! "${PYTHON}" -c "import pytest" >/dev/null 2>&1; then
    echo "ERROR: pytest is not installed for ${PYTHON}." >&2
    echo "Install dependencies first, e.g.:" >&2
    echo "    ${PYTHON} -m pip install -r requirements.txt pytest pytest-mock" >&2
    exit 1
fi

# With no arguments run the complete unit-test suite; otherwise forward the
# given files/node ids/options to pytest unchanged.
if [ "$#" -eq 0 ]; then
    set -- tests/
fi

echo "==> Running: $*"
# shellcheck disable=SC2086
exec "${PYTHON}" -m pytest -rf -v ${PYTEST_ARGS:-} "$@"
