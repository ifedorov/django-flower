# coding=utf-8
import logging

import gevent.monkey

gevent.monkey.patch_all()

from django.core.management import BaseCommand

from flower.events import Events, logger
from flower.options import options as settings


class Command(BaseCommand):

    def handle(self, *args, **options):
        events = Events(settings.app, settings)
        try:
            settings.app.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)
        events.run()
