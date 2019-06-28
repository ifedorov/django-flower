from __future__ import absolute_import

import logging

from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator

from flower.exceptions import HTTPError
from ..api.control import ControlHandler
from ..utils.broker import Broker
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class BrokerView(BaseHandler):

    @method_decorator(login_required)
    def get(self):
        app = self.settings.app
        broker_options = app.conf.BROKER_TRANSPORT_OPTIONS

        http_api = None
        if app.transport == 'amqp' and app.options.broker_api:
            http_api = app.options.broker_api

        try:
            broker = Broker(app.connection().as_uri(include_password=True),
                            http_api=http_api, broker_options=broker_options)
        except NotImplementedError:
            raise HTTPError(404, "'%s' broker is not supported" % app.transport)

        try:
            queue_names = ControlHandler.get_active_queue_names()
            if not queue_names:
                queue_names = set([app.conf.CELERY_DEFAULT_QUEUE]) | \
                              set([q.name for q in app.conf.CELERY_QUEUES or [] if q.name])
            queues = list(broker.queues(sorted(queue_names)))
        except Exception as e:
            raise HTTPError(404, "Unable to get queues: '%s'" % e)

        return self.render("broker.html",
                           context={
                               'broker_url': app.connection().as_uri(),
                               'queues': queues
                           })
