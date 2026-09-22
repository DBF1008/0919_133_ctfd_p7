"""Tests for production infrastructure configuration: DB pool tuning and SSE
client leak protections."""
import importlib

import CTFd.config as config_module
from CTFd.config import TestingConfig


def test_sqlite_has_no_queue_pool_options():
    """SQLite (default/test DB) uses a non-QueuePool and must not receive
    QueuePool-only options such as pool_size/max_overflow."""
    assert not hasattr(TestingConfig, "SQLALCHEMY_ENGINE_OPTIONS")


def test_default_sse_protections_present():
    """The SSE leak protections have safe defaults even for SQLite."""
    assert TestingConfig.SSE_CLIENT_QUEUE_MAXSIZE == 100
    assert TestingConfig.SSE_CLIENT_MAX_AGE == 300


def test_mysql_engine_options_complete(monkeypatch):
    """A MySQL URL must produce a complete, production-safe engine options
    dict. Values are provided through environment variables which
    EnvInterpolation resolves for empty config.ini entries."""
    monkeypatch.setenv("DATABASE_URL", "mysql+pymysql://ctfd:pw@db:3306/ctfd")
    monkeypatch.setenv("SQLALCHEMY_POOL_PRE_PING", "true")
    monkeypatch.setenv("SQLALCHEMY_POOL_RECYCLE", "280")
    monkeypatch.setenv("SQLALCHEMY_POOL_SIZE", "7")
    monkeypatch.setenv("SQLALCHEMY_MAX_OVERFLOW", "10")
    monkeypatch.setenv("SQLALCHEMY_POOL_TIMEOUT", "45")
    monkeypatch.setenv("SSE_CLIENT_QUEUE_MAXSIZE", "50")
    monkeypatch.setenv("SSE_CLIENT_MAX_AGE", "600")

    importlib.reload(config_module)
    try:
        opts = config_module.Config.SQLALCHEMY_ENGINE_OPTIONS
        assert opts["pool_pre_ping"] is True
        assert opts["pool_recycle"] == 280
        assert opts["pool_size"] == 7
        assert opts["max_overflow"] == 10
        assert opts["pool_timeout"] == 45
        assert config_module.Config.SSE_CLIENT_QUEUE_MAXSIZE == 50
        assert config_module.Config.SSE_CLIENT_MAX_AGE == 600
    finally:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("SQLALCHEMY_POOL_PRE_PING", raising=False)
        monkeypatch.delenv("SQLALCHEMY_POOL_RECYCLE", raising=False)
        monkeypatch.delenv("SQLALCHEMY_POOL_SIZE", raising=False)
        monkeypatch.delenv("SQLALCHEMY_MAX_OVERFLOW", raising=False)
        monkeypatch.delenv("SQLALCHEMY_POOL_TIMEOUT", raising=False)
        monkeypatch.delenv("SSE_CLIENT_QUEUE_MAXSIZE", raising=False)
        monkeypatch.delenv("SSE_CLIENT_MAX_AGE", raising=False)
        # Restore the module-level config consumed by the rest of the test run
        importlib.reload(config_module)


def test_mysql_engine_options_safe_defaults(monkeypatch):
    """Even with no tuning provided, MySQL gets pre-ping + recycle so stale
    connections cannot cause request 500s."""
    monkeypatch.setenv("DATABASE_URL", "mysql+pymysql://ctfd:pw@db:3306/ctfd")

    importlib.reload(config_module)
    try:
        opts = config_module.Config.SQLALCHEMY_ENGINE_OPTIONS
        assert opts["pool_pre_ping"] is True
        assert opts["pool_recycle"] == 280
        assert opts["pool_size"] == 5
        assert opts["max_overflow"] == 20
        assert opts["pool_timeout"] == 30
    finally:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        importlib.reload(config_module)
