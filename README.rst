Django Celery Flower
====================

Flower is a web based tool for monitoring and administrating Celery clusters.

Features
--------

- Real-time monitoring using Celery Events

    - Task progress and history
    - Ability to show task details (arguments, start time, runtime, and more)
    - Graphs and statistics

- Remote Control

    - View worker status and statistics
    - Shutdown and restart worker instances
    - Control worker pool size and autoscale settings
    - View and modify the queues a worker instance consumes from
    - View currently running tasks
    - View scheduled tasks (ETA/countdown)
    - View reserved and revoked tasks
    - Apply time and rate limits
    - Configuration viewer
    - Revoke or terminate tasks

- Broker monitoring

    - View statistics for all Celery queues
    - Queue length graphs

- HTTP API
- Basic Auth and Google OpenID authentication

API  (TODO)
-----------

Flower API enables to manage the cluster via REST API, call tasks and
receive task events in real-time via WebSockets.

For example you can restart worker's pool by: ::

    $ curl -X POST http://localhost:5555/api/worker/pool/restart/myworker

Or call a task by: ::

    $ curl -X POST -d '{"args":[1,2]}' http://localhost:5555/api/task/async-apply/tasks.add

Or terminate executing task by: ::

    $ curl -X POST -d 'terminate=True' http://localhost:5555/api/task/revoke/8a4da87b-e12b-4547-b89a-e92e4d1f8efd

Or receive task completion events in real-time:

.. code-block:: javascript 

    var ws = new WebSocket('ws://localhost:5555/api/task/events/task-succeeded/');
    ws.onmessage = function (event) {
        console.log(event.data);
    }

For more info checkout `API Reference`_ and `examples`_.

.. _API Reference: https://flower.readthedocs.io/en/latest/api.html
.. _examples: http://nbviewer.ipython.org/urls/raw.github.com/mher/flower/master/docs/api.ipynb

Installation
------------

PyPI version: ::

    $ pip install django-flower

Development version: ::

    $ pip install https://github.com/alexsilva/django-flower/zipball/master

Usage
-----

Add app flower to django installed apps: ::

    INSTALLED_APPS = [
        ...
        'flower'
    ]

Run the event command: ::

    $ python manage flower_events

Documentation
-------------

Everything that needs to be configured in the `sample project`_

.. _`sample project`: https://github.com/alexsilva/django-flower/blob/master/djproject

`flower_events`_ - is a django command that serves as a backend and should run in the background.

.. _`flower_events`: https://github.com/alexsilva/django-flower/blob/master/flower/management/commands/flower_events.py

Django settings variable: ::

    FLOWER_RPC_HOST
    FLOWER_RPC_PORT
    FLOWER_INSPECT_TIMEOUT
    FLOWER_AUTH
    FLOWER_BASIC_AUTH
    FLOWER_OAUTH2_KEY
    FLOWER_OAUTH2_SECRET
    FLOWER_OAUTH2_REDIRECT_URI
    FLOWER_MAX_WORKERS
    FLOWER_MAX_TASKS
    FLOWER_DB
    FLOWER_PERSISTENT
    FLOWER_BROKER_API
    FLOWER_CA_CERTS
    FLOWER_CERTFILE
    FLOWER_KEYFILE
    FLOWER_XHEADERS
    FLOWER_AUTO_REFRESH
    FLOWER_COOKIE_SECRET
    FLOWER_ENABLE_EVENTS
    FLOWER_FORMAT_TASK
    FLOWER_NATURAL_TIME
    FLOWER_TASKS_COLUMNS
    FLOWER_AUTH_PROVIDER
    FLOWER_INSPECT


License
-------

Flower is licensed under BSD 3-Clause License. See the LICENSE file
in the top distribution directory for the full license text.

Getting help
------------

Please head over to #celery IRC channel on irc.freenode.net or
`open an issue`_.

.. _open an issue: https://github.com/mher/flower/issues

Contributing
------------

If you'd like to contribute, simply fork `the repository`_, commit your
changes, run the tests (`tox`) and send a pull request.
Make sure you add yourself to CONTRIBUTORS_.

If you are interested in maintaining the project please contact.

.. _`the repository`: https://github.com/alexsilva/django-flower
.. _CONTRIBUTORS: https://github.com/alexsilva/django-flower/blob/master/CONTRIBUTORS
