"""Module to organise storage elements."""

import posixpath
import t2kdm as dm
from six import print_

class StorageElement(object):
    """Representation of a grid storage element"""

    def __init__(self, name, host, type, location, basepath, directpath=None, broken=False):
        """Initialise StorageElement.

        `name`: Identifier for element
        `host`: Hostname of element
        `type`: Storage type of element ('tape' or 'disk')
        `location`: Location of the SE, e.g. '/europe/uk/ral'
        `basepath`: Base path for standard storage paths on element
        `directpath`: Base path for direct access of data on the storage element (optional)
        `broken`: Is the SE broken and should not be used? Equivalent to a forced blacklisting.
        """

        self.name = name
        self.host = host
        self.basepath = basepath
        if directpath is None:
            self.directpath = basepath
        else:
            self.directpath = directpath
        self.location = location
        self.type = type
        self.broken = broken

    def is_blacklisted(self):
        """Is the SE blacklisted?"""
        return self.broken or self.name in dm.config.blacklist

    def get_storage_path(self, remotepath, direct=False):
        """Generate the standard storage path for this SE from a logical file name.

        Use the "directpath" instead of the basepath if `direct` is `True`.
        """
        if remotepath[0] != '/':
            raise ValueError("Remote path needs to be absolute, not relative!")
        if direct:
            return (self.directpath + dm.config.basedir + remotepath).strip()
        else:
            return (self.basepath + dm.config.basedir + remotepath).strip()

    def get_logical_path(self, surl):
        """Try to get the logical remotepath from a surl."""
        remotepath = None
        if surl.startswith(self.basepath):
            remotepath = surl[len(self.basepath):]
        if remotepath.startswith(dm.config.basedir):
            remotepath = remotepath[len(dm.config.basedir):]
        return remotepath

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
        for rep in dm.replicas(remotepath, cached=cached):
            if self.host in rep:
                return rep.strip()
        # Replica not found
        return None

    def has_replica(self, remotepath, cached=False, check_dark=False):
        """Check whether the remote path is replicated on this SE.

        If `check_dark` is `True`, check the physical file location, instead of relying on the catalogue.
        """
        if not check_dark:
            return any(self.host in replica for replica in dm.replicas(remotepath, cached=cached))
        else:
            return dm.is_file_se(remotepath, self, cached=cached)

    def get_closest_SE(self, remotepath=None, tape=False, cached=False):
        """Get the storage element with the closest replica.

        If `tape` is False (default), do not return any tape SEs.
        If no `rempotepath` is provided, just return the closest SE over all.
        """
        SEs = self.get_closest_SEs(remotepath=remotepath, tape=tape, cached=cached)
        if len(SEs) >= 1:
            return SEs[0]
        else:
            return None

    def get_closest_SEs(self, remotepath=None, tape=False, cached=False):
        """Get a list of the storage element with the closest replicas.

        If `tape` is False (default), do not return any tape SEs.
        If no `rempotepath` is provided, just return the closest SE over all.
        """
        on_tape = False

        if remotepath is None:
            candidates = SEs
        else:
            candidates = []
            for rep in dm.replicas(remotepath, cached=cached):
                cand = get_SE_by_path(rep)
                if cand is None:
                    continue
                if (cand.type == 'tape') and (tape == False):
                    on_tape = True
                    continue
                candidates.append(cand)

        if len(candidates) == 0 and on_tape:
            print_("WARNING: Replica only found on tape, but tape sources are not accepted!")

        def sorter(SE):
            if SE is None:
                return 1000
            distance = self.get_distance(SE)
            if SE.type == 'tape':
                # Prefer disks over tape, even if the tape is closer by
                distance += 10
            if SE.is_blacklisted():
                # Try blacklisted SEs only as a last resort
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

# Add actual SEs
SEs = [
    StorageElement('RAL-LCG2-T2K-tape',
        host = 'srm-t2k.gridpp.rl.ac.uk',
        type = 'tape',
        location = '/europe/uk/ral',
        basepath = 'srm://srm-t2k.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod'),
    StorageElement('UKI-SOUTHGRID-RALPP-disk',
        host = 'heplnx204.pp.rl.ac.uk',
        type = 'disk',
        location = '/europe/uk/ral',
        basepath = 'srm://heplnx204.pp.rl.ac.uk:8443/srm/managerv2?SFN=/pnfs/pp.rl.ac.uk/data/t2k'),
    StorageElement('UKI-SOUTHGRID-OX-HEP-disk',
        broken = True,
        host = 't2se01.physics.ox.ac.uk',
        type = 'disk',
        location = '/europe/uk/ox',
        basepath = 'srm://t2se01.physics.ox.ac.uk:8446/srm/managerv2?SFN=/dpm/physics.ox.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-SHEF-HEP-disk',
        broken = True,
        host = 'lcgse0.shef.ac.uk',
        type = 'disk',
        location = '/europe/uk/shef',
        basepath = 'srm://lcgse0.shef.ac.uk:8446/srm/managerv2?SFN=/dpm/shef.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-LANCS-HEP-disk',
        broken = True,
        host = 'fal-pygrid-30.lancs.ac.uk',
        type = 'disk',
        location = '/europe/uk/lancs',
        basepath = 'srm://fal-pygrid-30.lancs.ac.uk:8446/srm/managerv2?SFN=/dpm/lancs.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-MAN-HEP-disk',
        broken = True,
        host = 'bohr3226.tier2.hep.manchester.ac.uk',
        type = 'disk',
        location = '/europe/uk/man',
        basepath = 'root://bohr3226.tier2.hep.manchester.ac.uk:1094/dpm/tier2.hep.manchester.ac.uk/home/t2k.org'),
    StorageElement('UKI-NORTHGRID-LIV-HEP-disk',
        host = 'hepgrid11.ph.liv.ac.uk',
        type = 'disk',
        location = '/europe/uk/liv',
        basepath = 'srm://hepgrid11.ph.liv.ac.uk:8446/srm/managerv2?SFN=/dpm/ph.liv.ac.uk/home/t2k.org'),
    StorageElement('UKI-LT2-IC-HEP-disk',
        host = 'gfe02.grid.hep.ph.ic.ac.uk',
        type = 'disk',
        location = '/europe/uk/london/ic',
        basepath = 'srm://gfe02.grid.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/t2k'),
    StorageElement('UKI-LT2-QMUL2-disk',
        host = 'se03.esc.qmul.ac.uk',
        type = 'disk',
        location = '/europe/uk/london/qmul',
        directpath = 'root://xrootd.esc.qmul.ac.uk/t2k.org',
        basepath = 'srm://se03.esc.qmul.ac.uk:8444/srm/managerv2?SFN=/t2k.org'),
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
        basepath = 'srm://srm.pic.es:8443/srm/managerv2?SFN=/pnfs/pic.es/data/t2k.org'),
    StorageElement('CA-TRIUMF-T2K1-disk',
        host = 't2ksrm.nd280.org',
        type = 'disk',
        location = '/americas/ca/triumf',
        basepath = 'srm://t2ksrm.nd280.org:8443/srm/managerv2?SFN=/nd280data'),
    StorageElement('CA-SFU-T21-disk',
        host = 'lcg-t2kse1.sfu.computecanada.ca',
        type = 'disk',
        location = '/americas/ca/sfu',
        basepath = 'srm://lcg-t2kse1.sfu.computecanada.ca:8443/srm/managerv2?SFN=/nd280data'),
    StorageElement('JP-KEK-CRC-02-disk',
        host = 'kek2-se01.cc.kek.jp',
        type = 'disk',
        location = '/asia/jp/kek',
        basepath = 'srm://kek2-se01.cc.kek.jp:8444/srm/managerv2?SFN=/t2k.org'),
    StorageElement('JP-KEK-CRC-02-disk-old',
        broken = True,
        host = 'kek2-tmpse.cc.kek.jp',
        type = 'disk',
        location = '/asia/jp/kek',
        basepath = 'srm://kek2-tmpse.cc.kek.jp/dpm/cc.kek.jp/home/t2k.org'),
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


def get_closest_SEs(remotepath=None, location=None, tape=False, cached=False):
    """Get a list of the storage element with the closest replicas.

    If `tape` is False (default), do not return any tape SEs.
    If no `rempotepath` is provided, just return the closest SE over all.
    """

    if location is None:
        location = dm.config.location
        if location == '/':
            print_("WARNING:\nWARNING: Current location is '/'. Did you configure the location with `%s-config`?\nWARNING:"%(dm._branding,))

    # Create a pseudo SE with the correct location
    SE = StorageElement('local',
        host = 'localhost',
        type = 'disk',
        location = location,
        basepath = '/')

    return SE.get_closest_SEs(remotepath, tape=tape, cached=cached)

def get_closest_SE(remotepath=None, location=None, tape=False, cached=False):
    """Get the storage element with the closest replica.

    If `tape` is False (default), do not return any tape SEs.
    If no `rempotepath` is provided, just return the closest SE over all.
    """
    SEs = get_closest_SEs(remotepath=remotepath, tape=tape, cached=cached)
    if len(SEs) >= 1:
        return SEs[0]
    else:
        return None
