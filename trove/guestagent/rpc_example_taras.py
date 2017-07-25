import gettext

import sys

from oslo_config import cfg as openstack_cfg
from oslo_log import log as logging
from oslo_service import service as openstack_service

from trove.common import context as trove_context
from trove.common import cfg
from trove.common import debug_utils
from trove.common.i18n import _LE
from trove.guestagent import api as guest_api
from trove.common.db import models
from trove.common import notification
from trove.common.notification import StartNotification

CONF = cfg.CONF
def _make_request(path='/', context=None, **kwargs):
    from webob import Request
    path = '/'
    print("path: %s" % path)
    return Request.blank(path=path, environ={'trove.context': context},
                            **kwargs)

def main():
    action = None
    if len(sys.argv) > 1:
        action = sys.argv[1]
    
    cfg.parse_args(['ffs', '--config-file', '/etc/trove/trove-guestagent.conf'])

    # CONF.enable_secure_rpc_messaging = False
    logging.setup(CONF, None)
    debug_utils.setup()

    from trove import rpc
    rpc.init(CONF)

    import api


    context = trove_context.TroveContext()


    def persist_instance_fault(notification, event_qualifier):
        print "whhaaaa?"

    notification.DBaaSAPINotification.register_notify_callback(
         persist_instance_fault)
    # notification.DBaaSAPINotification.register_notify_callback(notify_callback)
    from trove.common.notification import NotificationCastWrapper

    a = api.API(context, "my_guest_id")
    if action == "prepare":
        a.prepare(128, "", [], [])
    elif action == "create_database":
        username = sys.argv[2]
        context.notification = notification.DBaaSInstanceCreate(context,
                                                                request=_make_request(context=context))
        with NotificationCastWrapper(context, 'guest'):
            print a.create_database([models.DatastoreSchema(name=username).serialize()])
    elif action == "create_user":
        username = sys.argv[2]
        print a.create_user([models.DatastoreUser(name=username, databases=[username]).serialize()])
    elif action == "list_users":
        print a.list_users()
        print a.list_databases()
    else:
        print "unknown action try one of:\n%s <prepare|create_user>" % (sys.argv[0])
        sys.exit(0)
main()
"""
Calling python rpc_example_taras.py create_database db
Results in trove-guestagent erroring:
2017-07-20 19:08:25.716 1408 DEBUG trove.common.rpc.service [-] Creating RPC server for service guestagent.my_guest_id start /trove/trove/common/rpc/service.py:57
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server [-] Exception during message handling: AttributeError: 'TroveContext' object has no attribute 'notification'
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server Traceback (most recent call last):
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server   File "/trove/.venv/local/lib/python2.7/site-packages/oslo_messaging/rpc/server.py", line 160, in _process_incoming
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server     res = self.dispatcher.dispatch(message)
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server   File "/trove/.venv/local/lib/python2.7/site-packages/oslo_messaging/rpc/dispatcher.py", line 213, in dispatch
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server     return self._do_dispatch(endpoint, method, ctxt, args)
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server   File "/trove/.venv/local/lib/python2.7/site-packages/oslo_messaging/rpc/dispatcher.py", line 183, in _do_dispatch
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server     result = func(ctxt, **new_args)
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server   File "/trove/.venv/local/lib/python2.7/site-packages/osprofiler/profiler.py", line 153, in wrapper
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server     return f(*args, **kwargs)
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server   File "/trove/trove/guestagent/datastore/experimental/postgresql/manager.py", line 126, in create_database
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server     with EndNotification(context):
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server   File "/trove/trove/common/notification.py", line 48, in __init__
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server     self.context.notification.payload.update(kwargs)
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server AttributeError: 'TroveContext' object has no attribute 'notification'
2017-07-20 19:08:32.405 1408 ERROR oslo_messaging.rpc.server

This is due to 
--- a/trove/guestagent/datastore/experimental/postgresql/manager.py
+++ b/trove/guestagent/datastore/experimental/postgresql/manager.py
@@ -123,8 +123,8 @@ class Manager(manager.Manager):
         self.app.start_db_with_conf_changes(context, config_contents)

     def create_database(self, context, databases):
-        with EndNotification(context):
^--- EndNotification seems to require something special from context to accept notifications
"""

