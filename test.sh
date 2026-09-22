#!/usr/bin/env bash
#
# test.sh - Manual test runner for the infrastructure hardening changes:
#   1. /healthz liveness probe and /readyz readiness probe
#   2. EventManager SSE client leak reaper / bounded queues
#   3. SQLAlchemy pool_pre_ping / pool_recycle / max_overflow configuration
#
# Usage:
#   ./test.sh                 Run the new unit tests via pytest
#   ./test.sh --all           Run the full test suite
#   ./test.sh --manual        Interactively probe a running CTFd server (curl)
#   ./test.sh --help
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PYTHON="${PYTHON:-python3}"
TARGET="http://localhost:4000"

NEW_TESTS=(
    "tests/test_health.py"
    "tests/utils/test_events.py"
    "tests/test_config.py"
)

print_header() {
    echo
    echo "=================================================================="
    echo " $1"
    echo "=================================================================="
}

usage() {
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

check_pytest() {
    if ! "$PYTHON" -c "import pytest" >/dev/null 2>&1; then
        echo "ERROR: pytest is not available for $PYTHON"
        echo "Install dependencies first, e.g.:  $PYTHON -m pip install -r requirements.txt"
        exit 1
    fi
}

run_unit_tests() {
    print_header "Running unit tests"
    check_pytest
    "$PYTHON" -m pytest -v "$@"
}

manual_probes() {
    print_header "Manual probe tests against ${TARGET}"
    echo "Start the server first, e.g.:  $PYTHON serve.py  (or gunicorn)"
    echo

    run_curl() {
        local path="$1"
        local desc="$2"
        echo "--- GET ${path}  (${desc})"
        curl -sS -o /tmp/ctfd_probe_body -w "HTTP %{http_code}\n" \
            --max-time 5 "${TARGET}${path}" || echo "curl failed (server down?)"
        echo "body: $(cat /tmp/ctfd_probe_body 2>/dev/null || true)"
        echo
    }

    run_curl "/healthz" "liveness: expect HTTP 200, body 'ok'"
    run_curl "/readyz"  "readiness: expect HTTP 200 'ok' (503 'error' while starting)"

    echo "Expected behavior:"
    echo "  /healthz always returns 200 once the process can accept connections."
    echo "  /readyz returns 503 during startup / import / DB outage, then 200."
    echo

    read -r -p "Run an SSE leak smoke test against /events? [y/N] " answer
    if [[ "${answer:-N}" =~ ^[Yy]$ ]]; then
        echo "Opening an SSE connection for ~7s (pings arrive every 5s)..."
        echo "Login first if auth is enabled; this raw check is best done pre-setup."
        curl -sS -N --max-time 7 "${TARGET}/events" || true
        echo
        echo "After the client vanishes (no FIN), the server-side queue must be"
        echo "reaped within ~SSE_CLIENT_IDLE_TIMEOUT+SSE_REAPER_INTERVAL seconds"
        echo "(defaults 35s + 10s). Watch server logs / memory to confirm."
    fi
}

case "${1:-}" in
    -h|--help)
        usage
        ;;
    --all)
        run_unit_tests tests
        ;;
    --manual)
        TARGET="${2:-$TARGET}"
        manual_probes
        ;;
    "")
        run_unit_tests "${NEW_TESTS[@]}"
        echo
        echo "For live endpoint checks run:  ./test.sh --manual [http://host:port]"
        ;;
    *)
        # Pass extra args straight through to pytest
        run_unit_tests "$@"
        ;;
esac
