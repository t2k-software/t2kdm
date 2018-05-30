"""T2K Data Manager

Helpful tools to manage the T2K data on the grid.
"""

import backends

backend = backends.LCGBackend()

ls = backend.ls
replicas = backend.replicas
replicate = backend.replicate
