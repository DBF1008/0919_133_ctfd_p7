from unittest.mock import patch

from CTFd.utils.health import check_cache, check_database_connection
from tests.helpers import create_ctfd, destroy_ctfd, login_as_user, register_user


def test_healthz_ok_without_auth_or_setup():
    """Liveness probe must answer 200 without authentication and independent of
    database/cache health (a dependency outage must never restart the pod)."""
    app = create_ctfd()
    with app.test_client() as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.get_json() == {"status": "ok"}
        assert r.headers["Cache-Control"] == "no-store"
    destroy_ctfd(app)


def test_healthz_still_ok_when_database_down():
    """/healthz only reports process liveness and does not touch the database."""
    app = create_ctfd()
    with app.test_client() as client:
        with patch(
            "CTFd.probes.check_database_connection", return_value=False
        ):
            r = client.get("/healthz")
        assert r.status_code == 200
    destroy_ctfd(app)


def test_readyz_ok_when_healthy():
    """Readiness probe reports ready once started and dependencies are up."""
    app = create_ctfd()
    with app.test_client() as client:
        r = client.get("/readyz")
        assert r.status_code == 200
        body = r.get_json()
        assert body["status"] == "ok"
        assert body["checks"] == {"startup": True, "database": True, "cache": True}
        assert r.headers["Cache-Control"] == "no-store"
    destroy_ctfd(app)


def test_readyz_503_before_startup_complete():
    """A pod still starting must not receive load-balancer traffic."""
    app = create_ctfd()
    app.ready = False
    with app.test_client() as client:
        r = client.get("/readyz")
        assert r.status_code == 503
        body = r.get_json()
        assert body["status"] == "unavailable"
        assert body["checks"]["startup"] is False
    destroy_ctfd(app)


def test_readyz_503_when_database_unreachable():
    """A dead database must flip readiness to not-ready without killing the pod."""
    app = create_ctfd()
    with app.test_client() as client:
        with patch(
            "CTFd.probes.check_database_connection", return_value=False
        ):
            r = client.get("/readyz")
        assert r.status_code == 503
        assert r.get_json()["checks"]["database"] is False
    destroy_ctfd(app)


def test_readyz_503_when_cache_unreachable():
    app = create_ctfd()
    with app.test_client() as client:
        with patch("CTFd.probes.check_cache", return_value=False):
            r = client.get("/readyz")
        assert r.status_code == 503
        assert r.get_json()["checks"]["cache"] is False
    destroy_ctfd(app)


def test_probes_available_without_setup_redirect():
    """Probes must not redirect to /setup on a fresh, unconfigured instance."""
    app = create_ctfd(setup=False)
    with app.test_client() as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code in (200, 503)
    destroy_ctfd(app)


def test_probes_no_auth_required():
    """Probes must not require login and should not create tracking rows."""
    app = create_ctfd()
    with app.app_context():
        register_user(app)
        with login_as_user(app) as client:
            assert client.get("/healthz").status_code == 200
            assert client.get("/readyz").status_code == 200
        # Anonymous access as kubelet has no session/cookie
        with app.test_client() as anon:
            assert anon.get("/healthz").status_code == 200
            assert anon.get("/readyz").status_code == 200
    destroy_ctfd(app)


def test_check_database_connection_true_on_healthy_db():
    app = create_ctfd()
    with app.app_context():
        assert check_database_connection() is True
    destroy_ctfd(app)


def test_check_database_connection_false_on_error():
    app = create_ctfd()
    with app.app_context():
        with patch(
            "CTFd.utils.health.db.engine.connect",
            side_effect=Exception("connection refused"),
        ):
            assert check_database_connection() is False
    destroy_ctfd(app)


def test_check_cache_filesystem_always_ok():
    app = create_ctfd()
    with app.app_context():
        assert check_cache() is True
    destroy_ctfd(app)
