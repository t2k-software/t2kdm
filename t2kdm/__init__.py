"""T2K Data Manager

Helpful tools to manage the T2K data on the grid.
"""

import backends

backend = backends.LCGBackend(basedir='/t2k.org')

ls = backend.ls
replicas = backend.replicas
