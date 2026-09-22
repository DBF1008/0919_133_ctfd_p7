from time import time

from flask import current_app, has_app_context
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import database_exists

from CTFd.cache import cache, timed_lru_cache
from CTFd.utils import import_in_progress

# Process-wide readiness flag. It flips to True only once create_app() has
# finished every startup task (db migrations, plugin loading, blueprint
# registration...). Load balancer readiness probes should poll /readyz so that
# traffic is only sent to a fully started worker.
_ready = False


def set_ready():
    global _ready
    _ready = True


def is_ready():
    return _ready


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
    """
    Actively verify that a checked out database connection can execute a
    trivial query. Unlike check_database() this catches a stale/dead pooled
    connection. Intentionally not cached so /readyz reflects reality.
    """
    from CTFd.models import db

    try:
        db.session.execute(text("SELECT 1"))
        db.session.rollback()
        return True
    except SQLAlchemyError:
        db.session.remove()
        return False


def check_startup_complete():
    """
    Ready only when startup is finished, no CTF export/import is in progress
    and an application context is available.
    """
    if is_ready() is False:
        return False
    if has_app_context() and import_in_progress():
        return False
    return True
