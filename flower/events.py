from __future__ import absolute_import
from __future__ import with_statement

import collections
import logging
import time

import gevent.monkey

gevent.monkey.patch_all()

from celery.events import EventReceiver
from celery.events.state import State
from rpyc.utils.server import GeventServer
from rpyc.utils.helpers import classpartial
import rpyc

try:
    from collections import Counter
except ImportError:
    from .utils.backports.collections import Counter

logger = logging.getLogger(__name__)


class CeleryStateService(rpyc.SlaveService):

    def __init__(self, state):
        super(CeleryStateService, self).__init__()
        self.state = state

    def exposed_get_state(self):
        return self.state


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super(EventsState, self).__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)

    def event(self, event):
        worker_name = event['hostname']
        event_type = event['type']

        self.counter[worker_name][event_type] += 1

        # Send event to api subscribers (via websockets)
        # classname = api.events.getClassName(event_type)
        # cls = getattr(api.events, classname, None)
        # if cls:
        #     cls.send_message(event)

        # Save the event
        return super(EventsState, self).event(event)


class Events(object):

    def __init__(self, app, options):
        self.state = EventsState()
        self.options = options
        self.app = app
        self.server = None

    def start_rpc(self):
        service = classpartial(CeleryStateService, self.state)
        self.server = GeventServer(service,
                                   hostname='localhost',
                                   port=6002,
                                   auto_register=False)
        self.server._listen()
        gevent.spawn(self.server.start)
        return self.server

    def run(self):
        self.start_rpc()
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                with self.app.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_shutter},
                                         app=self.app)
                    try_interval = 1
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        try:
            self.app.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)

    def on_shutter(self, event):
        self.state.event(event)

        if not self.state.event_count:
            # No new events since last snapshot.
            return
