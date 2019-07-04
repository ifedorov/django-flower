from __future__ import absolute_import

import logging

from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator

from ..api.workers import ListWorkers
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class WorkerView(BaseHandler):

    @method_decorator(login_required)
    def get(self, request, name):
        try:
            ListWorkers.update_workers(settings=self.settings,
                                       workername=name)
        except Exception as e:
            logger.error(e)

        worker = ListWorkers.worker_cache.get(name)

        if worker is None:
            return self.write_error(404, message="Unknown worker '%s'" % name)

        if 'stats' not in worker:
            return self.write_error(404, message="Unable to get stats for '%s' worker" % name)

        context = {
            'worker': dict(worker, name=name)
        }
        return self.render("flower/worker.html", context=context)
