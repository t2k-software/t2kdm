"""HyperK Data Manager

Helpful tools to manage the HyperK data on the grid.
"""

__version__ = '1.9.0'

import configuration
from configuration import _branding
import backends
import storage
import utils
import sys

if sys.argv[0].endswith('%s-config'%(_branding,)):
    # Someone is calling <brandname>-config.
    # Do not try to load any configuration, as that is what they might be trying to fix!
    pass
else:
    # Load configuration
    config = configuration.load_config()
    # Get backend according to configuration
    backend = backends.get_backend(config)

    # Get functions from backend
    ls = backend.ls
    iter_ls = backend.iter_ls
    ls_se = backend.ls_se
    iter_ls_se = backend.iter_ls_se
    is_dir = backend.is_dir
    is_dir_se = backend.is_dir_se
    replicas = backend.replicas
    is_file = backend.is_file
    is_file_se = backend.is_file_se
    exists = backend.exists
    is_online = backend.is_online
    checksum = backend.checksum
    state = backend.state
    replicate = backend.replicate
    remove = backend.remove
    rmdir = backend.rmdir
    move = backend.move
    rename = backend.rename
    get = backend.get
    put = backend.put

    # And from utils
    check_checksums = utils.check_checksums
    check_replicas = utils.check_replicas
    check_replica_states = utils.check_replica_states
