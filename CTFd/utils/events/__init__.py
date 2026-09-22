import json
import time
import weakref
from collections import defaultdict
from queue import Empty, Full, Queue

from gevent import GreenletExit, Timeout, getcurrent, sleep, spawn
from tenacity import retry, wait_exponential

from CTFd.cache import cache
from CTFd.utils import string_types


# Interval (in seconds) at which the SSE loop sends ping frames.
PING_INTERVAL = 5

# Default maximum number of undelivered events buffered per client before the
# oldest events are dropped. Prevents slow/dead clients from consuming unbounded
# memory.
DEFAULT_MAX_QUEUE_SIZE = 100

# Default maximum lifetime (in seconds) of a single SSE connection. When the
# limit is reached the client is disconnected; EventSource clients reconnect
# transparently. This bounds how long a half-open connection whose TCP death
# was never observed can stay in ``clients``.
DEFAULT_MAX_CLIENT_AGE = 300


class ServerSentEvent(object):
    def __init__(self, data, type=None, id=None):
        self.data = data
        self.type = type
        self.id = id

    def __str__(self):
        if isinstance(self.data, string_types):
            data = self.data
        else:
            data = json.dumps(self.data)
        lines = ["data:{value}".format(value=line) for line in data.splitlines()]
        if self.type:
            lines.insert(0, "event:{value}".format(value=self.type))
        if self.id:
            lines.append("id:{value}".format(value=self.id))
        return "\n".join(lines) + "\n\n"

    def to_dict(self):
        d = {"data": self.data}
        if self.type:
            d["type"] = self.type
        if self.id:
            d["id"] = self.id
        return d


def _bounded_defaultdict_queue(maxsize):
    def queue_factory():
        return Queue(maxsize=maxsize)

    return defaultdict(queue_factory)


def _enqueue(q, message):
    """Put a message on a bounded queue, dropping the oldest item if it is full."""
    try:
        q.put_nowait(message)
    except Full:
        try:
            q.get_nowait()
        except Empty:
            pass
        try:
            q.put_nowait(message)
        except Full:
            pass


class EventManager(object):
    def __init__(self, max_queue_size=DEFAULT_MAX_QUEUE_SIZE, max_client_age=None):
        # ``clients`` maps client id -> defaultdict(channel -> Queue) and keeps
        # its historical shape so that publishers (and the Redis listener) can
        # treat every value as a dict of per-channel queues.
        self.clients = {}
        # Metadata (creation timestamp, owning greenlet) keyed by the same id.
        self._client_meta = {}
        self.max_queue_size = max_queue_size
        self.max_client_age = max_client_age
        self._reaper = None

    def publish(self, data, type=None, id=None, channel="ctf"):
        event = ServerSentEvent(data, type=type, id=id)
        message = event.to_dict()
        for client in list(self.clients.values()):
            _enqueue(client[channel], message)
        return len(self.clients)

    def _reap_stale_clients(self):
        """Disconnect SSE clients that have exceeded the maximum connection age."""
        if not self.max_client_age:
            return
        now = time.monotonic()
        stale_ids = [
            client_id
            for client_id, meta in list(self._client_meta.items())
            if now - meta["created_at"] >= self.max_client_age
        ]
        for client_id in stale_ids:
            meta = self._client_meta.pop(client_id, None)
            self.clients.pop(client_id, None)
            target = meta["greenlet"] if meta else None
            # Killing the subscription greenlet runs the generator ``finally``
            # block and tears down the WSGI response even if the TCP peer
            # vanished without ever sending a FIN.
            if target is not None and target != getcurrent() and not target.dead:
                target.kill(GreenletExit, block=False)

    def _start_reaper(self):
        if not self.max_client_age:
            return
        if self._reaper is not None and not self._reaper.dead:
            return
        # Sweep promptly but not aggressively: by default every ~30s, capped so
        # that small age limits (and tests) still reap on time.
        interval = min(30, max(1, self.max_client_age // 10))
        weak_self = weakref.ref(self)

        def reaper_loop():
            while True:
                sleep(interval)
                manager = weak_self()
                if manager is None:
                    return
                manager._reap_stale_clients()

        self._reaper = spawn(reaper_loop)

    def listen(self):
        self._start_reaper()

    def subscribe(self, channel="ctf"):
        q = _bounded_defaultdict_queue(self.max_queue_size)
        client_id = id(q)
        self.clients[client_id] = q
        self._client_meta[client_id] = {
            "created_at": time.monotonic(),
            "greenlet": getcurrent(),
        }
        try:
            # Immediately yield a ping event to force Response headers to be set
            # or else some reverse proxies will incorrectly buffer SSE
            yield ServerSentEvent(data="ping", type="ping")
            while True:
                with Timeout(PING_INTERVAL, False):
                    message = q[channel].get()
                    yield ServerSentEvent(**message)
                yield ServerSentEvent(data="ping", type="ping")
        finally:
            self._remove_client(client_id)

    def _remove_client(self, client_id):
        self.clients.pop(client_id, None)
        self._client_meta.pop(client_id, None)


class RedisEventManager(EventManager):
    def __init__(self, max_queue_size=DEFAULT_MAX_QUEUE_SIZE, max_client_age=None):
        super(RedisEventManager, self).__init__(
            max_queue_size=max_queue_size, max_client_age=max_client_age
        )
        self.client = cache.cache._write_client

    def publish(self, data, type=None, id=None, channel="ctf"):
        event = ServerSentEvent(data, type=type, id=id)
        message = json.dumps(event.to_dict())
        return self.client.publish(message=message, channel=channel)

    def listen(self, channel="ctf"):
        self._start_reaper()

        @retry(wait=wait_exponential(min=1, max=30))
        def _listen():
            while True:
                pubsub = self.client.pubsub()
                pubsub.subscribe(channel)
                try:
                    while True:
                        message = pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=5
                        )
                        if message:
                            if message["type"] == "message":
                                event = json.loads(message["data"])
                                for client in list(self.clients.values()):
                                    _enqueue(client[channel], event)
                finally:
                    pubsub.close()

        spawn(_listen)
