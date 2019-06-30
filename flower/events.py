from __future__ import absolute_import
from __future__ import with_statement

import collections
import logging

from celery.events.snapshot import Polaroid
from celery.events.state import State

from flower.models import CeleryWorker, CeleryTask

try:
    from collections import Counter
except ImportError:
    from .utils.backports.collections import Counter

logger = logging.getLogger(__name__)


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


class Events(Polaroid):

    def run(self, freq=1.0):
        state = EventsState()
        with self.app.connection() as connection:
            recv = self.app.events.Receiver(connection, handlers={'*': state.event})
            with self.__class__(state, freq=freq):
                recv.capture(limit=None, timeout=None)

    def enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        try:
            self.app.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)

    def on_shutter(self, state):
        if not state.event_count:
            # No new events since last snapshot.
            return
        tasks = state.tasks
        for key in tasks.keys():
            task = tasks[key]
            defaults = {
                'name': task.name,
                'state': task.state,
                'worker': task.worker.id
            }
            obj, created = CeleryTask.objects.get_or_create(pk=key, defaults=defaults)
            if not created:
                for name, value in defaults.iteritems():
                    setattr(obj, name, value)
            obj.save()

        workers = self.state.workers
        for key in workers.keys():
            worker = workers[key]
            defaults = {
                'name': worker.hostname,
                'alive': worker.alive
            }
            obj, created = CeleryWorker.objects.get_or_create(pk=key, defaults=defaults)
            if not created:
                for name, value in defaults.iteritems():
                    setattr(obj, name, value)
            obj.save()
