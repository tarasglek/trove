import gettext
gettext.install('trove', unicode=1)

import sys

from oslo_config import cfg as openstack_cfg
from oslo_log import log as logging
from oslo_service import service as openstack_service

from trove.common import cfg
from trove.common import debug_utils
from trove.common.i18n import _LE
from trove.guestagent import api as guest_api

CONF = cfg.CONF
# The guest_id opt definition must match the one in common/cfg.py
CONF.register_opts([openstack_cfg.StrOpt('guest_id', default=None,
                                         help="ID of the Guest Instance."),
                    openstack_cfg.StrOpt('instance_rpc_encr_key',
                                         help=('Key (OpenSSL aes_cbc) for '
                                               'instance RPC encryption.'))])

def main():
    cfg.parse_args(['ffs', '--config-file', '/etc/trove/trove-guestagent.conf'])

    # CONF.enable_secure_rpc_messaging = False
    logging.setup(CONF, None)
    debug_utils.setup()

    from trove import rpc
    rpc.init(CONF)

    from trove.common import context as trove_context
    import api
    context = trove_context.TroveContext()
    a = api.API(context, "my_guest_id")
    print a.list_databases()
    print a.enable_root()
main()
