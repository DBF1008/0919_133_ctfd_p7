from time import time

from flask import current_app
from sqlalchemy import text
from sqlalchemy_utils import database_exists

from CTFd.cache import cache, timed_lru_cache
from CTFd.models import db


@timed_lru_cache(timeout=30)
def check_database():
    return database_exists(current_app.config["SQLALCHEMY_DATABASE_URI"])


@timed_lru_cache(timeout=30)
def check_config():
    key = "healthcheck"
    value = round(time() / 5) * 5
    cache.set(key, value)
    return cache.get(key) == value


def check_database_connection():
    """Verify a real round-trip to the database.

    Unlike ``check_database`` this is not cached and actually borrows a
    connection from the pool so that readiness probes detect stale or dropped
    connections. With ``pool_pre_ping`` enabled the borrowed connection is
    validated (and transparently recycled) by SQLAlchemy.
    """
    try:
        with db.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        current_app.logger.exception("Database readiness check failed")
        return False


def check_cache():
    """Verify the cache backend is reachable. The filesystem cache is local and
    always considered available."""
    cache_type = current_app.config.get("CACHE_TYPE")
    if cache_type not in ("redis", "RedisCache", "flask_caching.backends.rediscache.RedisCache"):
        return True
    try:
        cache.set("readyz", 1)
        return cache.get("readyz") == 1
    except Exception:
        current_app.logger.exception("Cache readiness check failed")
        return False
