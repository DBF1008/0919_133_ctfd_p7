"""Kubernetes-style liveness (/healthz) and readiness (/readyz) probe endpoints.

``/healthz`` only verifies that the process is alive and able to serve HTTP; it
is suitable as a liveness probe and must never depend on external systems so
that a transient database outage does not cause containers to be restarted.

``/readyz`` verifies that the application finished starting and that its
dependencies (database, and Redis when it is configured) are reachable. A pod
reporting not-ready is kept alive but removed from load-balancer backends.
"""
from flask import Blueprint, current_app, jsonify

from CTFd.utils.health import check_cache, check_database_connection

probes = Blueprint("probes", __name__)


@probes.route("/healthz")
def healthz():
    response = jsonify({"status": "ok"})
    response.headers["Cache-Control"] = "no-store"
    return response, 200


@probes.route("/readyz")
def readyz():
    checks = {}

    checks["startup"] = bool(getattr(current_app, "ready", False))
    checks["database"] = check_database_connection()
    checks["cache"] = check_cache()

    response = jsonify(
        {
            "status": "ok" if all(checks.values()) else "unavailable",
            "checks": checks,
        }
    )
    response.headers["Cache-Control"] = "no-store"
    return response, (200 if all(checks.values()) else 503)
