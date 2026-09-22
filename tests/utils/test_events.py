from collections import defaultdict
from queue import Queue
from unittest.mock import patch

from redis.exceptions import ConnectionError

from CTFd.config import TestingConfig
from CTFd.utils.events import EventManager, RedisEventManager, ServerSentEvent
from tests.helpers import create_ctfd, destroy_ctfd, login_as_user, register_user


def test_event_manager_installed():
    """Test that EventManager is installed on the Flask app"""
    app = create_ctfd()
    assert type(app.events_manager) == EventManager
    destroy_ctfd(app)


def test_event_manager_subscription():
    """Test that EventManager subscribing works"""
    with patch.object(Queue, "get") as fake_queue:
        saved_data = {
            "user_id": None,
            "title": "asdf",
            "content": "asdf",
            "team_id": None,
            "user": None,
            "team": None,
            "date": "2019-01-28T01:20:46.017649+00:00",
            "id": 10,
        }
        saved_event = {"type": "notification", "data": saved_data}

        fake_queue.return_value = saved_event
        event_manager = EventManager()
        events = event_manager.subscribe()
        message = next(events)
        assert isinstance(message, ServerSentEvent)
        assert message.to_dict() == {"data": "ping", "type": "ping"}
        assert message.__str__().startswith("event:ping")
        assert len(event_manager.clients) == 1

        message = next(events)
        assert isinstance(message, ServerSentEvent)
        assert message.to_dict() == saved_event
        assert message.__str__().startswith("event:notification\ndata:")
        assert len(event_manager.clients) == 1


def test_event_manager_publish():
    """Test that EventManager publishing to clients works"""
    saved_data = {
        "user_id": None,
        "title": "asdf",
        "content": "asdf",
        "team_id": None,
        "user": None,
        "team": None,
        "date": "2019-01-28T01:20:46.017649+00:00",
        "id": 10,
    }

    event_manager = EventManager()
    q = defaultdict(Queue)
    event_manager.clients[id(q)] = q
    event_manager.publish(data=saved_data, type="notification", channel="ctf")

    event = event_manager.clients[id(q)]["ctf"].get()
    event = ServerSentEvent(**event)
    assert event.data == saved_data


def test_event_endpoint_is_event_stream():
    """Test that the /events endpoint is text/event-stream"""
    app = create_ctfd()
    with patch.object(Queue, "get") as fake_queue:
        saved_data = {
            "user_id": None,
            "title": "asdf",
            "content": "asdf",
            "team_id": None,
            "user": None,
            "team": None,
            "date": "2019-01-28T01:20:46.017649+00:00",
            "id": 10,
        }
        saved_event = {"type": "notification", "data": saved_data}

        fake_queue.return_value = saved_event
        with app.app_context():
            register_user(app)
            with login_as_user(app) as client:
                r = client.get("/events")
                assert "text/event-stream" in r.headers["Content-Type"]
    destroy_ctfd(app)


def test_redis_event_manager_installed():
    """Test that RedisEventManager is installed on the Flask app"""

    class RedisConfig(TestingConfig):
        REDIS_URL = "redis://localhost:6379/1"
        CACHE_REDIS_URL = "redis://localhost:6379/1"
        CACHE_TYPE = "redis"

    try:
        app = create_ctfd(config=RedisConfig)
    except ConnectionError:
        print("Failed to connect to redis. Skipping test.")
    else:
        with app.app_context():
            assert isinstance(app.events_manager, RedisEventManager)
        destroy_ctfd(app)


def test_redis_event_manager_subscription():
    """Test that RedisEventManager subscribing works."""

    class RedisConfig(TestingConfig):
        REDIS_URL = "redis://localhost:6379/2"
        CACHE_REDIS_URL = "redis://localhost:6379/2"
        CACHE_TYPE = "redis"

    try:
        app = create_ctfd(config=RedisConfig)
    except ConnectionError:
        print("Failed to connect to redis. Skipping test.")
    else:
        with app.app_context():
            saved_data = {
                "user_id": None,
                "title": "asdf",
                "content": "asdf",
                "team_id": None,
                "user": None,
                "team": None,
                "date": "2019-01-28T01:20:46.017649+00:00",
                "id": 10,
            }
            saved_event = {"type": "notification", "data": saved_data}

            with patch.object(Queue, "get") as fake_queue:
                fake_queue.return_value = saved_event
                event_manager = RedisEventManager()

                events = event_manager.subscribe()
                message = next(events)
                assert isinstance(message, ServerSentEvent)
                assert message.to_dict() == {"data": "ping", "type": "ping"}
                assert message.__str__().startswith("event:ping")

                message = next(events)
                assert isinstance(message, ServerSentEvent)
                assert message.to_dict() == saved_event
                assert message.__str__().startswith("event:notification\ndata:")
        destroy_ctfd(app)


def test_redis_event_manager_publish():
    """Test that RedisEventManager publishing to clients works."""

    class RedisConfig(TestingConfig):
        REDIS_URL = "redis://localhost:6379/3"
        CACHE_REDIS_URL = "redis://localhost:6379/3"
        CACHE_TYPE = "redis"

    try:
        app = create_ctfd(config=RedisConfig)
    except ConnectionError:
        print("Failed to connect to redis. Skipping test.")
    else:
        with app.app_context():
            saved_data = {
                "user_id": None,
                "title": "asdf",
                "content": "asdf",
                "team_id": None,
                "user": None,
                "team": None,
                "date": "2019-01-28T01:20:46.017649+00:00",
                "id": 10,
            }

            event_manager = RedisEventManager()
            event_manager.publish(data=saved_data, type="notification", channel="ctf")
        destroy_ctfd(app)


def test_redis_event_manager_listen():
    """Test that RedisEventManager listening pubsub works."""
    # This test is nob currently working properly
    # This test is sort of incomplete b/c we aren't also subscribing
    # I wasnt able to get listening and subscribing to work at the same time
    # But the code does work under gunicorn and serve.py
    try:
        # import importlib
        # from gevent.monkey import patch_time, patch_socket
        # from gevent import Timeout

        # patch_time()
        # patch_socket()

        class RedisConfig(TestingConfig):
            REDIS_URL = "redis://localhost:6379/4"
            CACHE_REDIS_URL = "redis://localhost:6379/4"
            CACHE_TYPE = "redis"

        try:
            app = create_ctfd(config=RedisConfig)
        except ConnectionError:
            print("Failed to connect to redis. Skipping test.")
        else:
            with app.app_context():
                # saved_event = {
                #     "data": {
                #         "team_id": None,
                #         "user_id": None,
                #         "content": "asdf",
                #         "title": "asdf",
                #         "id": 1,
                #         "team": None,
                #         "user": None,
                #         "date": "2020-08-31T23:57:27.193081+00:00",
                #         "type": "toast",
                #         "sound": None,
                #     },
                #     "type": "notification",
                # }

                event_manager = RedisEventManager()

                # def disable_retry(f, *args, **kwargs):
                #     return f()

                # with patch("tenacity.retry", side_effect=disable_retry):
                #     with Timeout(10):
                #         event_manager.listen()
                event_manager.listen()

                # event_manager.publish(
                #     data=saved_event["data"], type="notification", channel="ctf"
                # )
            destroy_ctfd(app)
    finally:
        pass
        # import socket
        # import time

        # importlib.reload(socket)
        # importlib.reload(time)


def test_event_manager_unsubscribe_removes_client():
    """unsubscribe() forcefully removes a client queue from the manager."""
    event_manager = EventManager()
    client_id, _ = event_manager._subscribe_queue()

    assert event_manager.client_count() == 1
    assert client_id in event_manager._client_meta

    event_manager.unsubscribe(client_id)

    assert event_manager.client_count() == 0
    assert client_id not in event_manager.clients
    assert client_id not in event_manager._client_meta


def test_event_manager_generator_close_cleans_up_client():
    """Closing a subscriber generator runs cleanup even without exhausting it."""
    event_manager = EventManager()
    events = event_manager.subscribe()

    next(events)
    assert event_manager.client_count() == 1

    # Simulates the WSGI server closing the response when the client disconnects
    events.close()

    assert event_manager.client_count() == 0


def test_event_manager_reaps_idle_client():
    """A client that made no progress past the idle timeout is evicted."""
    event_manager = EventManager()
    client_id, _ = event_manager._subscribe_queue()
    event_manager._client_meta[client_id]["last_active"] = 0
    # Avoid killing the main test greenlet when the stale client is evicted
    event_manager._client_meta[client_id]["greenlet"] = None

    assert event_manager.client_count() == 1

    event_manager._reap()

    assert event_manager.client_count() == 0
    assert client_id not in event_manager._client_meta


def test_event_manager_does_not_reap_active_client():
    """A client that recently made progress is left alone by the reaper."""
    event_manager = EventManager()
    client_id, _ = event_manager._subscribe_queue()

    event_manager._reap()

    assert event_manager.client_count() == 1
    assert client_id in event_manager._client_meta


def test_event_manager_drops_events_for_full_client_queue():
    """Bounded queues stop a dead consumer from growing memory without bound."""
    event_manager = EventManager()
    client_id, q = event_manager._subscribe_queue()
    queue = q["ctf"]

    delivered = 0
    for i in range(queue.maxsize + 10):
        delivered += event_manager._deliver(q, "ctf", {"data": i})

    # Only maxsize messages were accepted; the rest were dropped for this
    # client instead of accumulating forever.
    assert delivered == queue.maxsize
    assert queue.qsize() == queue.maxsize


def test_event_manager_reaps_client_with_full_backlog():
    """A client whose total backlog exceeds the queue limit is considered dead."""
    event_manager = EventManager()
    client_id, q = event_manager._subscribe_queue()
    event_manager._client_meta[client_id]["greenlet"] = None

    # Fill two channels so the aggregate backlog exceeds CLIENT_QUEUE_SIZE
    # even though each per-channel bounded queue caps itself at the limit.
    for i in range(q["ctf"].maxsize):
        event_manager._deliver(q, "ctf", {"data": i})
    event_manager._deliver(q, "other", {"data": 1})

    assert sum(queue.qsize() for queue in q.values()) > q["ctf"].maxsize

    event_manager._reap()

    assert event_manager.client_count() == 0
    assert client_id not in event_manager._client_meta


def test_publish_never_blocks_on_dead_client():
    """Publishing to a full/dead queue must not raise or block."""
    event_manager = EventManager()
    client_id, q = event_manager._subscribe_queue()
    for i in range(q["ctf"].maxsize + 5):
        event_manager.publish(data=i, type="notification", channel="ctf")

    assert q["ctf"].qsize() == q["ctf"].maxsize
    event_manager.unsubscribe(client_id)
