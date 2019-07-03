# coding=utf-8
import gevent.monkey

gevent.monkey.patch_all()

from django.core.management import BaseCommand

from flower.events import Events
from flower.options import options as settings


class Command(BaseCommand):

    def handle(self, *args, **options):
        events = Events(settings.app, settings)
        events.run()
