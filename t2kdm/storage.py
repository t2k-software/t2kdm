"""Module to organise storage elements."""

import posixpath
import t2kdm

class StorageElement(object):
    """Representation of a grid storage element"""

    def __init__(self, name, host, type, location, basepath):
        """Initialise StorageElement.

        `name`: Identifier for element
        `host`: Hostname of element
        `type`: Storage type of element ('tape' or 'disk')
        `location`: Location of the SE, e.g. '/europe/uk/ral'
        `basepath`: Base path for standard storage paths on element
        """

        self.name = name
        self.host = host
        self.basepath = basepath
        self.location = location
        self.type = type

    def get_storage_path(self, remotepath):
        """Generate the standard storage path for this SE from a logical file name."""
        if remotepath[0] != '/':
            raise ValueError("Remote path needs to be absolute, not relative!")
        return (self.basepath + remotepath).strip()

    def get_distance(self, other):
        """Return the distance to another StorageElement.

        Returns a negative number. The smaller (i.e. more negative) it is,
        the closer the two SE are together.
        """

        common = posixpath.commonprefix([self.location.lower()+'/', other.location.lower()+'/'])
        # The more '/' are in the common prefix, the closer the SEs are.
        # So we can take the negative number as measure of distance.
        distance = -common.count('/')
        return distance

    def get_replica(self, remotepath):
        """Return the replica of the file on this SM."""
        for rep in t2kdm.replicas(remotepath, _iter=True):
            if self.host in rep:
                return rep.strip()
        # Replica not found
        return None

    def has_replica(self, remotepath):
        """Check whether the remote path is replicated on this SE."""
        return self.host in t2kdm.replicas(remotepath)

    def get_closest_SE(self, remotepath, tape=False):
        """Get the storage element with the closest replica.

        If `tape` is False (default), prefer disk SEs over tape SEs.
        """
        closest_SE = None
        closest_distance = None
        for rep in t2kdm.replicas(remotepath, _iter=True):
            SE = get_SE_by_path(rep)
            if SE is None:
                continue
            if closest_SE is None:
                # Always accept the first SE
                closest_SE = SE
                closest_distance = 0 # Distances are negative, so this is the farthest it can get
            elif SE.type != 'tape':
                # We always accept non-tape SEs
                if self.get_distance(SE) <= closest_distance:
                    # Also when they are equally close as the current choice
                    closest_SE = SE
                    closest_distance = self.get_distance(SE)
            elif tape or closest_SE.type == 'tape':
                # We accept the candidate tape SEs
                # Either bu choice or because the current best candidate is already a tape SE
                if self.get_distance(SE) < closest_distance:
                    # But only if it is *better* than the current choice
                    closest_SE = SE
                    closest_distance = self.get_distance(SE)

        return closest_SE

    def __str__(self):
        return "%s (%s)"%(self.name, self.host)

class TriumfStorageElement(StorageElement):
    """Special case of StorageElement for TRIUMF.

    Storage file paths do not translate one-to-one to logical file paths,
    so we have to catch these differences.
    """

    def get_storage_path(self, remotepath):
        """Generate the standard storage path for this SE from a logical file name."""

        # ND280 data is in the sub folder `nd280data`
        if remotepath.startswith('/nd280/'):
            return StorageElement.get_storage_path(self, '/nd280data/' + remotepath[7:])

        #Everything else seems to be one-to-one
        return StorageElement.get_storage_path(self, remotepath)

# Add actual SEs
SEs = [
    StorageElement('RAL-LCG22-tape',
        host = 'srm-t2k.gridpp.rl.ac.uk',
        type = 'tape',
        location = '/europe/uk/ral',
        basepath = 'srm://srm-t2k.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/t2k.org'),
    StorageElement('UKI-SOUTHGRID-RALPP-disk',
        host = 'heplnx204.pp.rl.ac.uk',
        type = 'disk',
        location = '/europe/uk/ral',
        basepath = 'srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/t2k/t2k.org'),
    StorageElement('UKI-NORTHGRID-SHEF-HEP-disk',
        host = 'lcgse0.shef.ac.uk',
        type = 'disk',
        location = '/europe/uk/shef',
        basepath = 'srm://lcgse0.shef.ac.uk/dpm/shef.ac.uk/home/t2k.org'),
    TriumfStorageElement('CA-TRIUMF-T2K1-disk',
        host = 't2ksrm.nd280.org',
        type = 'disk',
        location = '/americas/ca/triumf',
        basepath = 'srm://t2ksrm.nd280.org'),
    StorageElement('JP-KEK-CRC-02-disk',
        host = 'kek2-se01.cc.kek.jp',
        type = 'disk',
        location = '/asia/jp/kek',
        basepath = 'srm://'),
# TODO:
#GRIF-disk
#GridPPSandboxSE
#IFIC-LCG2-disk
#IN2P3-CC-disk
#INFN-BARI1-disk
#Nebraska1-disk
#UKI-LT2-IC-HEP-disk
#UKI-LT2-QMUL2-disk
#UKI-NORTHGRID-LANCS-HEP-disk
#UKI-NORTHGRID-LIV-HEP-disk
#UKI-NORTHGRID-MAN-HEP-disk
#UKI-SOUTHGRID-OX-HEP-disk
#UNIBE-LHEP-disk
#pic-disk
    ]

SE_by_name = {}
SE_by_host = {}

for SE in SEs:
    SE_by_name[SE.name] = SE
    SE_by_host[SE.host] = SE

def get_SE_by_path(path):
    """Return the StorageElement corresponsing to the given srm-path."""
    for SE in SEs:
        if SE.host in path:
            return SE
    return None

def get_SE(SE):
    """Get the StorageElement by all means necessary."""
    if SE in SE_by_name:
        return SE_by_name[SE]
    if SE in SE_by_host:
        return SE_by_host[SE]
    return get_SE_by_path(SE)

def get_closest_SE(remotepath, location=None, tape=False):
    """Get the closest storage element with a replica of the given file.

    If `tape` is False (default), prefer disk SEs over tape SEs.
    """

    if location is None:
        location = t2kdm.config.location

    # Create a psude SE with the correct location
    SE = StorageElement('local',
        host = 'localhost',
        type = 'disk',
        location = location,
        basepath = '/')

    return SE.get_closest_SE(remotepath, tape=tape)
