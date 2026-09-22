#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the /healthz liveness and /readyz readiness probe endpoints."""

from unittest.mock import patch

from CTFd.utils import health
from tests.helpers import create_ctfd, destroy_ctfd


def _clear_probe_caches():
    health.check_database.cache_clear()
    health.check_config.cache_clear()


def test_healthz_returns_ok_when_running():
    """The liveness probe returns 200 as long as the process can serve."""
    app = create_ctfd()
    with app.app_context():
        client = app.test_client()
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.get_data(as_text=True) == "ok"
        assert r.headers["Cache-Control"] == "no-store"
        assert r.mimetype == "text/plain"
    destroy_ctfd(app)
    _clear_probe_caches()


def test_healthz_available_without_setup():
    """/healthz must not redirect to /setup on a fresh, unconfigured instance."""
    app = create_ctfd(setup=False)
    with app.app_context():
        client = app.test_client()
        r = client.get("/healthz")
        assert r.status_code == 200
    destroy_ctfd(app)
    _clear_probe_caches()


def test_healthz_available_when_database_down():
    """Liveness never reports failure for dependency outages (avoids restart loops)."""
    app = create_ctfd()
    with app.app_context():
        client = app.test_client()
        with patch("CTFd.utils.health.database_exists", return_value=False):
            health.check_database.cache_clear()
            r = client.get("/healthz")
            assert r.status_code == 200
    destroy_ctfd(app)
    _clear_probe_caches()


def test_readyz_returns_ok_when_ready():
    """After startup the readiness probe returns 200 and reports ok."""
    app = create_ctfd()
    with app.app_context():
        client = app.test_client()
        r = client.get("/readyz")
        assert r.status_code == 200
        assert r.get_data(as_text=True) == "ok"
        assert r.headers["Cache-Control"] == "no-store"
    destroy_ctfd(app)
    _clear_probe_caches()


def test_readyz_available_without_setup():
    """Readiness reflects dependencies/startup, not the CTF setup wizard."""
    app = create_ctfd(setup=False)
    with app.app_context():
        # setup=False leaves create_app() fully run, so the process is ready
        # even though the setup wizard has not been completed.
        client = app.test_client()
        r = client.get("/readyz")
        assert r.status_code == 200
    destroy_ctfd(app)
    _clear_probe_caches()


def test_readyz_returns_503_before_startup_complete():
    """/readyz fails until the application explicitly marks itself ready."""
    app = create_ctfd()
    health._ready = False
    try:
        with app.app_context():
            client = app.test_client()
            r = client.get("/readyz")
            assert r.status_code == 503
            assert r.get_data(as_text=True) == "error"
    finally:
        health.set_ready()
    destroy_ctfd(app)
    _clear_probe_caches()


def test_readyz_returns_503_when_database_unreachable():
    """A failing database connectivity check must take the pod out of rotation."""
    app = create_ctfd()
    with app.app_context():
        client = app.test_client()
        with patch("CTFd.utils.health.database_exists", return_value=False):
            health.check_database.cache_clear()
            r = client.get("/readyz")
            assert r.status_code == 503
    destroy_ctfd(app)
    _clear_probe_caches()


def test_readyz_returns_503_during_import():
    """A CTF import in progress means the instance cannot serve traffic."""
    app = create_ctfd()
    with app.app_context():
        client = app.test_client()
        with patch("CTFd.utils.health.import_in_progress", return_value=True):
            r = client.get("/readyz")
            assert r.status_code == 503
    destroy_ctfd(app)
    _clear_probe_caches()
