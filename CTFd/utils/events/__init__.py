import json
import logging
import os
from collections import defaultdict
from queue import Full, Queue
from time import time

from gevent import GreenletExit, Timeout, getcurrent, sleep, spawn
from gevent.hub import get_hub
from tenacity import retry, wait_exponential

from CTFd.cache import cache
from CTFd.utils import string_types


# After this many seconds without the subscription loop making progress the
# client is considered dead (e.g. its TCP connection vanished without a FIN so
# no write error was ever raised) and its queue is reaped.
CLIENT_IDLE_TIMEOUT = int(os.environ.get("SSE_CLIENT_IDLE_TIMEOUT", 35))

# Maximum number of undelivered events buffered per client. A consumer that
# falls this far behind (typically because the connection is dead) is reaped
# instead of buffering events forever.
CLIENT_QUEUE_SIZE = int(os.environ.get("SSE_QUEUE_SIZE", 100))

# How often the background reaper scans for dead clients.
REAPER_INTERVAL = int(os.environ.get("SSE_REAPER_INTERVAL", 10))

logger = logging.getLogger(__name__)


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


class MonitoredQueue(Queue):
    """
    Bounded Queue that records when it was last drained. The reaper uses this to
    distinguish a live-but-idle consumer from a dead connection whose buffer is
    only growing. The bound also stops a single dead client from consuming
    unbounded memory.
    """

    def __init__(self, maxsize=CLIENT_QUEUE_SIZE):
        super(MonitoredQueue, self).__init__(maxsize=maxsize)
        self.last_active = time()

    def get(self, *args, **kwargs):
        item = super(MonitoredQueue, self).get(*args, **kwargs)
        self.last_active = time()
        return item

    def get_nowait(self):
        item = super(MonitoredQueue, self).get_nowait()
        self.last_active = time()
        return item


class EventManager(object):
    def __init__(self):
        self.clients = {}
        # Per-client bookkeeping (last progress timestamp, owning greenlet).
        # Kept separate from ``clients`` so the public clients/queue structure
        # stays backwards compatible.
        self._client_meta = {}
        self._reaper_started = False

    def publish(self, data, type=None, id=None, channel="ctf"):
        event = ServerSentEvent(data, type=type, id=id)
        message = event.to_dict()
        delivered = 0
        for client in list(self.clients.values()):
            delivered += self._deliver(client, channel, message)
        return delivered

    def _deliver(self, client, channel, message):
        """
        Non-blocking enqueue. A dead/stuck consumer must never block the
        publisher or grow its queue without bound; when the bounded queue is
        full the event is dropped for that client and the reaper evicts it on
        its next sweep.
        """
        try:
            client[channel].put_nowait(message)
        except Full:
            return 0
        return 1

    def _unsubscribe(self, client_id):
        self.clients.pop(client_id, None)
        self._client_meta.pop(client_id, None)

    def unsubscribe(self, client_id):
        """Forcefully remove a subscribed client queue."""
        self._unsubscribe(client_id)

    def client_count(self):
        return len(self.clients)

    def _reap_loop(self):
        while True:
            sleep(REAPER_INTERVAL)
            try:
                self._reap()
            except Exception:
                logger.exception("Error while reaping stale SSE clients")

    def _reap(self):
        """
        Evict clients whose subscription loop has not made progress within
        CLIENT_IDLE_TIMEOUT or whose event backlog has grown past
        CLIENT_QUEUE_SIZE. This is the safety net for clients that disappear
        without sending a FIN: their generator finally block never runs, so
        without the reaper their Queue would stay in ``clients`` forever and
        accumulate events.
        """
        now = time()
        for client_id, meta in list(self._client_meta.items()):
            stale = now - meta["last_active"] > CLIENT_IDLE_TIMEOUT
            client = self.clients.get(client_id)
            if client is None:
                stale = True
            elif sum(queue.qsize() for queue in client.values()) > CLIENT_QUEUE_SIZE:
                stale = True

            if stale is False:
                continue

            greenlet = meta.get("greenlet")
            self._unsubscribe(client_id)
            # Killing the blocked greenlet raises GreenletExit even while it is
            # stuck writing to a half-open socket, which unwinds the generator
            # and runs its finally block.
            if (
                greenlet is not None
                and greenlet.dead is False
                and greenlet is not get_hub()
            ):
                try:
                    greenlet.kill(GreenletExit, block=False)
                except Exception:
                    logger.exception("Unable to kill stale SSE client greenlet")

    def listen(self):
        # Start the dead-client reaper exactly once per manager instance.
        if self._reaper_started is False:
            self._reaper_started = True
            spawn(self._reap_loop)

    def _subscribe_queue(self, channel="ctf"):
        q = defaultdict(MonitoredQueue)
        client_id = id(q)
        self.clients[client_id] = q
        self._client_meta[client_id] = {
            "last_active": time(),
            "greenlet": getcurrent(),
        }
        return client_id, q

    def subscribe(self, channel="ctf"):
        client_id, q = self._subscribe_queue(channel)
        try:
            # Immediately yield a ping event to force Response headers to be set
            # or else some reverse proxies will incorrectly buffer SSE
            yield ServerSentEvent(data="ping", type="ping")
            while True:
                with Timeout(5, False):
                    message = q[channel].get()
                    yield ServerSentEvent(**message)
                # Reaching this point means the loop is alive (an event arrived
                # or the 5s ping timeout elapsed). The reaper uses this
                # timestamp to detect generators blocked forever on dead
                # connections, whose finally block never otherwise executes.
                if client_id in self._client_meta:
                    self._client_meta[client_id]["last_active"] = time()
                yield ServerSentEvent(data="ping", type="ping")
        finally:
            self._unsubscribe(client_id)


class RedisEventManager(EventManager):
    def __init__(self):
        super(RedisEventManager, self).__init__()
        self.client = cache.cache._write_client

    def publish(self, data, type=None, id=None, channel="ctf"):
        event = ServerSentEvent(data, type=type, id=id)
        message = json.dumps(event.to_dict())
        return self.client.publish(message=message, channel=channel)

    def listen(self, channel="ctf"):
        super(RedisEventManager, self).listen()
        spawn(self._listen, channel)

    def _listen(self, channel="ctf"):
        @retry(wait=wait_exponential(min=1, max=30))
        def listen_loop():
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
                                    self._deliver(client, channel, event)
                finally:
                    pubsub.close()

        listen_loop()
