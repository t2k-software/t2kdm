"""Module to organise storage elements."""

import posixpath
import t2kdm
from six import print_

class StorageElement(object):
    """Representation of a grid storage element"""

    def __init__(self, name, host, type, location, basepath, broken=False):
        """Initialise StorageElement.

        `name`: Identifier for element
        `host`: Hostname of element
        `type`: Storage type of element ('tape' or 'disk')
        `location`: Location of the SE, e.g. '/europe/uk/ral'
        `basepath`: Base path for standard storage paths on element
        `broken`: Is the SE broken and should not be used? Equivalent to a forced blacklisting.
        """

        self.name = name
        self.host = host
        self.basepath = basepath
        self.location = location
        self.type = type
        self.broken = broken

    def is_blacklisted(self):
        """Is the SE blacklisted?"""
        return self.broken or self.name in t2kdm.config.blacklist

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

    def get_replica(self, remotepath, cached=False):
        """Return the replica of the file on this SM."""
        for rep in t2kdm.replicas(remotepath, cached=cached):
            if self.host in rep:
                return rep.strip()
        # Replica not found
        return None

    def has_replica(self, remotepath, cached=False):
        """Check whether the remote path is replicated on this SE."""
        return any(self.host in replica for replica in t2kdm.replicas(remotepath, cached=cached))

    def get_closest_SE(self, remotepath=None, tape=False, cached=False):
        """Get the storage element with the closest replica.

        If `tape` is False (default), prefer disk SEs over tape SEs.
        If no `rempotepath` is provided, just return the closest SE over all.
        """
        SEs = self.get_closest_SEs(remotepath=remotepath, tape=tape, cached=cached)
        if len(SEs) >= 1:
            return SEs[0]
        else:
            return None

    def get_closest_SEs(self, remotepath=None, tape=False, cached=False):
        """Get a list of the storage element with the closest replicas.

        If `tape` is False (default), prefer disk SEs over tape SEs.
        If no `rempotepath` is provided, just return the closest SE over all.
        """
        closest_SE = None
        closest_distance = None

        if remotepath is None:
            candidates = SEs
        else:
            candidates = []
            for rep in t2kdm.replicas(remotepath, cached=cached):
                cand = get_SE_by_path(rep)
                if cand is not None:
                    candidates.append(cand)

        def sorter(SE):
            if SE is None:
                return 1000
            distance = self.get_distance(SE)
            if SE.type == 'tape':
                if tape:
                    distance += 0.5
                else:
                    distance += 10
            if SE.is_blacklisted():
                distance += 100
            return distance

        return sorted(candidates, key=sorter)

    def __str__(self):
        if self.broken:
            return "%s (%s) [%s] --> BROKEN! <--"%(self.name, self.host, self.location)
        elif self.is_blacklisted():
            return "%s (%s) [%s] --> BLACKLISTED <--"%(self.name, self.host, self.location)
        else:
            return "%s (%s) [%s]"%(self.name, self.host, self.location)

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
    StorageElement('RAL-LCG2-T2K-tape',
        host = 'srm-t2k.gridpp.rl.ac.uk',
        type = 'tape',
        location = '/europe/uk/ral',
        basepath = 'srm://srm-t2k.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/t2k.org'),
    StorageElement('UKI-SOUTHGRID-RALPP-disk',
        host = 'heplnx204.pp.rl.ac.uk',
        type = 'disk',
        location = '/europe/uk/ral',
        basepath = 'srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/t2k/t2k.org'),
    StorageElement('UKI-SOUTHGRID-OX-HEP-disk',
        host = 't2se01.physics.ox.ac.uk',
        type = 'disk',
        location = '/europe/uk/ox',
        basepath = 'srm://t2se01.physics.ox.ac.uk/dpm/physics.ox.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-SHEF-HEP-disk',
        host = 'lcgse0.shef.ac.uk',
        type = 'disk',
        location = '/europe/uk/shef',
        basepath = 'srm://lcgse0.shef.ac.uk/dpm/shef.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-LANCS-HEP-disk',
        host = 'fal-pygrid-30.lancs.ac.uk',
        type = 'disk',
        location = '/europe/uk/lancs',
        basepath = 'srm://fal-pygrid-30.lancs.ac.uk/dpm/lancs.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-MAN-HEP-disk',
        host = 'bohr3226.tier2.hep.manchester.ac.uk',
        type = 'disk',
        location = '/europe/uk/man',
        basepath = 'srm://bohr3226.tier2.hep.manchester.ac.uk/dpm/tier2.hep.manchester.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-LIV-HEP-disk',
        host = 'hepgrid11.ph.liv.ac.uk',
        type = 'disk',
        location = '/europe/uk/liv',
        basepath = 'srm://hepgrid11.ph.liv.ac.uk/dpm/ph.liv.ac.uk/home/t2k.org'),
    StorageElement('UKI-LT2-IC-HEP-disk',
        host = 'gfe02.grid.hep.ph.ic.ac.uk',
        type = 'disk',
        location = '/europe/uk/london/ic',
        basepath = 'srm://gfe02.grid.hep.ph.ic.ac.uk/pnfs/hep.ph.ic.ac.uk/data/t2k'),
    StorageElement('UKI-LT2-QMUL2-disk',
        host = 'se03.esc.qmul.ac.uk',
        type = 'disk',
        location = '/europe/uk/london/qmul',
        basepath = 'srm://se03.esc.qmul.ac.uk/t2k.org'),
    StorageElement('IN2P3-CC-disk',
        broken = True,
        host = 'in2p3.fr',
        type = 'disk',
        location = '/europe/fr/in2p3',
        basepath = 'srm://polgrid4.in2p3.fr/dpm/in2p3.fr/home/t2k.org'),
    StorageElement('pic-disk',
        host = 'srm.pic.es',
        type = 'disk',
        location = '/europe/es/pic',
        basepath = 'srm://srm.pic.es/pnfs/pic.es/data/t2k.org'),
    #StorageElement('IFIC-LCG2-disk',
    #    host = 'srmv2.ific.uv.es',
    #    type = 'disk',
    #    location = '/europe/es/ific',
    #    basepath = ''), # Need to know base path
    #StorageElement('INFN-BARI1-disk',
    #    host = 'storm-se-01.ba.infn.it',
    #    type = 'disk',
    #    location = '/europe/',
    #    basepath = ''), # Need to know base path
    TriumfStorageElement('CA-TRIUMF-T2K1-disk',
        host = 't2ksrm.nd280.org',
        type = 'disk',
        location = '/americas/ca/triumf',
        basepath = 'srm://t2ksrm.nd280.org'),
    StorageElement('JP-KEK-CRC-02-disk',
        host = 'kek2-se01.cc.kek.jp',
        type = 'disk',
        location = '/asia/jp/kek',
        basepath = 'srm://kek2-se01.cc.kek.jp/t2k.org'),
    StorageElement('JP-KEK-CRC-02-disk-old',
        broken = True,
        host = 'kek2-tmpse.cc.kek.jp',
        type = 'disk',
        location = '/asia/jp/kek',
        basepath = 'srm://kek2-tmpse.cc.kek.jp/dpm/cc.kek.jp/home/t2k.org'),
# TODO (Possibly Dirac SEs that are not available to T2K in general):
#GRIF-disk
#GridPPSandboxSE
#Nebraska1-disk
#UNIBE-LHEP-disk
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
    if isinstance(SE, StorageElement):
        return SE
    if SE in SE_by_name:
        return SE_by_name[SE]
    if SE in SE_by_host:
        return SE_by_host[SE]
    return get_SE_by_path(SE)

def get_closest_SE(remotepath=None, location=None, tape=False, cached=False):
    """Get the closest storage element with a replica of the given file.

    If `tape` is False (default), prefer disk SEs over tape SEs.
    If no `rempotepath` is provided, just return the closest SE over all.
    """

    if location is None:
        location = t2kdm.config.location
        if location == '/':
            print_("WARNING:\nWARNING: Current location is '/'. Did you configure the location with `t2kdm-config`?\nWARNING:")

    # Create a pseudo SE with the correct location
    SE = StorageElement('local',
        host = 'localhost',
        type = 'disk',
        location = location,
        basepath = '/')

    return SE.get_closest_SE(remotepath, tape=tape, cached=cached)
