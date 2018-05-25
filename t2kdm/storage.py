"""Module to organise storage elements."""

import posixpath

SE_by_name = {}
SE_by_host = {}

class StorageElement(object):
    """Representation of a grid storage element"""

    def __init__(self, name, host, type, basepath):
        """Initialise StorageElement.

        `name`: Identifier for element
        `host`: Hostname of element
        `type`: Storage type of element ('tape' or 'disk')
        `basepath`: Base path for standard storage paths on element
        """

        self.name = name
        self.host = host
        self.basepath = basepath
        self.type = type

    def get_storage_path(self, remotepath):
        """Generate the standard storage path for this SE from a logical file name."""
        return posixpath.join(self.basepath, remotepath)

# Add actual SEs
SEs = [ #name                           #host                       #type
            #basepath
        StorageElement(
        'CA-TRIUMF-T2K1-disk',          't2ksrm.nd280.org',         'disk',
            'srm://'),
        StorageElement(
        'RAL-LCG22-tape',               'srm-t2k.gridpp.rl.ac.uk',  'tape',
            'srm://srm-t2k.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/t2k.org'),
        StorageElement(
        'UKI-SOUTHGRID-RALPP-disk',     'heplnx204.pp.rl.ac.uk',    'disk',
            'rm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/t2k/t2k.org'),
        StorageElement(
        'JP-KEK-CRC-02-disk',           'kek2-se01.cc.kek.jp',      'disk',
            'srm://'),
    ]

def get_SE_by_path(path):
    """Return the StorageElement corresponsing to the given srm-path."""
    for SE in SEs:
        if SE.host in path:
            return SE
    return None
