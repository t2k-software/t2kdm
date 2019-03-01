"""T2K Data Manager

Helpful tools to manage the T2K data on the grid.
"""

import configuration
import backends
import storage
import utils
import sys

if sys.argv[0].endswith('t2kdm-config'):
    # Someone is calling t2kdm-config.
    # Do not try to load any configuration, as that is what they might be trying to fix!
    pass
else:
    # Load configuration
    config = configuration.load_config()
    # Get backend according to configuration
    backend = backends.get_backend(config)

    # Get functions from backend
    ls = backend.ls
    ls_se = backend.ls_se
    is_dir = backend.is_dir
    replicas = backend.replicas
    exists = backend.exists
    checksum = backend.checksum
    state = backend.state
    replicate = backend.replicate
    remove = backend.remove
    get = backend.get
    put = backend.put

    # And from utils
    check_checksums = utils.check_checksums
    check_replicas = utils.check_replicas
    check_replica_states = utils.check_replica_states
