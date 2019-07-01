from __future__ import absolute_import

import logging

from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator

from flower.exceptions import HTTPError
from ..api.workers import ListWorkers
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class WorkerView(BaseHandler):

    @method_decorator(login_required)
    def get(self, name):
        try:
            yield ListWorkers.update_workers(app=self.capp, workername=name)
        except Exception as e:
            logger.error(e)

        worker = ListWorkers.worker_cache.get(name)

        if worker is None:
            raise HTTPError(404, "Unknown worker '%s'" % name)
        if 'stats' not in worker:
            raise HTTPError(404, "Unable to get stats for '%s' worker" % name)

        self.render("worker.html", context={
            'worker': dict(worker, name=name)
        })
