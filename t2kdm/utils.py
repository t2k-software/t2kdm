"""Module for convenience functions that go beyond the basic backend capabilities."""

import posixpath
from copy import deepcopy
from six import print_
import os, sys, sh
import tempfile
from contextlib import contextmanager
import re
import t2kdm
from t2kdm import backends
from t2kdm import storage

def remote_iter_recursively(remotepath, regex=None, se=None, ignore_exceptions=False):
    """Iter over remote paths recursively.

    If `regex` is given, only consider files/folders that match the reular expression.
    If `se` is given, iterate over listing of physical files on SE rather than the file catalogue.
    If `ignore_exceptions` is `True`, exceptions are ignored where possible.
    """

    if isinstance(regex, str):
        regex = re.compile(regex)

    # Check whther the path is a directory.
    # Check both in the file catalogue and on the storage element,
    # because the directory might not yet exist in the catalogue.
    try:
        isdir = t2kdm.is_dir(remotepath, cached=True) or (se is not None and t2kdm.is_dir_se(remotepath, se, cached=True))
    except Exception as e:
        print_("Recursion failure!")
        if ignore_exceptions:
            print_(e)
        else:
            raise
        return

    if isdir:
        try:
            if se is None:
                entries = t2kdm.iter_ls(remotepath)
            else:
                entries = t2kdm.iter_ls_se(remotepath, se)
        except Exception as e:
            print_("Recursion failure!")
            if ignore_exceptions:
                print_(e)
            else:
                raise
            return
        for entry in entries:
            if regex is None or regex.search(entry.name):
                new_path = posixpath.join(remotepath, entry.name)
                for path in remote_iter_recursively(new_path, regex, se=se, ignore_exceptions=ignore_exceptions):
                    yield path
    else:
        yield remotepath

def check_checksums(remotepath, cached=False):
    """Check if the checksums of all replicas are identical."""

    replicas = t2kdm.replicas(remotepath, cached=cached)
    checksum = t2kdm.checksum(replicas[0], cached=cached)

    if '?' in checksum:
        return False

    for rep in replicas[1:]:
        if t2kdm.checksum(rep, cached=cached) != checksum:
            return False

    return True

def check_replica_states(remotepath, cached=False):
    """Check if the state of all replicas."""

    replicas = t2kdm.replicas(remotepath, cached=cached)

    for rep in replicas:
        if t2kdm.state(rep, cached=cached) not in ['ONLINE', 'NEARLINE', 'ONLINE_AND_NEARLINE']:
            return False

    return True

def check_replicas(remotepath, ses, cached=False):
    """Check whether the file is replcated to the given SE(s)."""

    check_ses = []

    for se in ses:
        if isinstance(se, str):
            se_obj = storage.get_SE(se)
            if se_obj is None:
                raise backends.BackendException("Not a valid storage element: %s"%(se,))
            else:
                se = se_obj
        check_ses.append(se)

    for se in check_ses:
        if not se.has_replica(remotepath, check_dark=False, cached=cached):
            return False
        if not se.has_replica(remotepath, check_dark=True, cached=cached):
            return False

    return True

def fix_known_bad_SEs(remotepath, verbose=False):
    """Fix replicas on known broken storage elements.

    Unregisters all replicas on them.
    """

    success = True

    replicas = t2kdm.replicas(remotepath)
    for replica in replicas:
        se = storage.get_SE(replica)
        if se is None:
            print_("Found replica on unknown storage element: "+replica)
            success = False
        elif se.broken:
            if verbose:
                print_("Found replica on bad storage element. Unregistering replica: "+replica)
            try:
                t2kdm.backend.deregister(replica, remotepath, verbose=verbose)
            except backends.BackendException():
                if verbose:
                    print_("Failed to deregister replica.")
                success = False

    return success

def fix_missing_files(remotepath, verbose=False):
    """Fix missing files on the storage elements.

    Helps when a replica is registered in the catalogue, but not actually present on the SE.
    """

    success = True

    replicas = t2kdm.replicas(remotepath)
    existing = []
    for rep in replicas:
        se = storage.get_SE(rep)
        if se is not None and se.is_blacklisted():
            if verbose:
                print_("WARNING: Skipping replica on blacklisted SE: "+rep)
                print_("Will assume it exists for now.")
            exists = True
            success = False
        else:
            try:
                exists = t2kdm.exists(rep)
            except backends.BackendException:
                if verbose:
                    print_("WARNING: Could not check whether replica exists: "+rep)
                    print_("Will assume it does for now.")
                exists = True
                success = False
        existing.append(exists)

    # Check that there is at least one replica actually present
    if not any( existing ):
        if verbose:
            print_("WARNING: There is not a single replica actually present!")
            print_("Doing nothing.")
        return False

    # Remove the replicas that are not present and remember which SEs those were
    ses = []
    for replica, exists in zip(replicas, existing):
        if not exists:
            if verbose:
                print_("Found missing file. Unregistering replica: "+replica)
            se = storage.get_SE(replica)
            if se is not None:
                ses.append(se)
                try:
                    t2kdm.backend.deregister(replica, remotepath, verbose=verbose)
                except backends.BackendException():
                    if verbose:
                        print_("Failed to deregister replica.")
                    success = False
            else:
                print_("Cannot identify storage element of replica.")
                success = False

    # Replicate the file
    for se in ses:
        if verbose:
            print_("Replicating missing replica on " + se.name)
        try:
            t2kdm.replicate(remotepath, se, verbose=verbose)
        except backends.BackendException():
            if verbose:
                print_("Failed to replicate File.")
            success = False

    return success

def _test_replica(replica, verbose=False):
    """Test whether a replica has the checksum it reports and whether it passes the gzip test."""

    tempdir = tempfile.mkdtemp()
    tempf = os.path.join(tempdir, 'temp.gz')

    try:
        if verbose:
            print_("Downloading and checking replica: "+replica)
        t2kdm.backend._get(replica, tempf, verbose=verbose)

        remote_checksum = t2kdm.checksum(replica)
        local_checksum = sh.adler32(tempf).strip()

        if local_checksum != remote_checksum:
            if verbose:
                print_(replica)
                print_("Local checksum %s is different from remote checksum %s."%(local_checksum, remote_checksum))
            return False

        try:
            sh.gzip(tempf, test=True)
        except sh.ErrorReturnCode:
            if verbose:
                print_(replica)
                print_("Failed the gzip integrity test.")
            return False
        else:
            return True

    finally:
        sh.rm('-f', tempf)
        sh.rmdir(tempdir)

def fix_checksum_errors(remotepath, verbose=False):
    """Fix replicas with differing checksums.

    This can only be done for files that can be checked for corruption.
    Otherwise there is no way to decide which file is actually the correct one.
    """

    replicas = t2kdm.replicas(remotepath)
    checksums = [t2kdm.checksum(r) for r in replicas]

    if len(set(checksums)) == 1 and '?' not in checksums[0]:
        # Nothing to do here
        return True

    if verbose:
        print_("Found faulty checksums.")

    if not remotepath.endswith('.gz'):
        if verbose:
            print_("WARNING: Can only check file consistency of *.gz files!")
            print_("Doing nothing.")
        return False

    good_replicas = []
    bad_replicas = []
    for replica in replicas:
        if _test_replica(replica, verbose=verbose):
            good_replicas.append(replica)
        else:
            bad_replicas.append(replica)

    if len(good_replicas) == 0:
        if verbose:
            print_("WARNING: Not a single good replica present!")
            print_("Doing nothing.")
        return False

    if len(bad_replicas) == 0:
        if verbose:
            print_("WARNING: Not a single bad replica present!")
            print_("This should not happen, since the checksums are different.")
            print_("Doing nothing.")
        return False

    bad_SEs = []
    for replica in bad_replicas:
        SE = storage.get_SE(replica)
        if SE is None:
            if verbose:
                print_("WARNING: Could not find storage element for replica: "+replica)
            continue
        bad_SEs.append(SE)

    success = True

    for SE in bad_SEs:
        if verbose:
            print_("Removing bad replica from %s."%(SE.name,))
        try:
            t2kdm.remove(remotepath, SE, verbose=verbose)
        except:
            success = False

    for SE in bad_SEs:
        if verbose:
            print_("Re-replicating file on %s."%(SE.name,))
        try:
            t2kdm.replicate(remotepath, SE, verbose=verbose)
        except:
            success = False

    return success

def fix_all(remotepath, verbose=False):
    """Try to automatically fix some common issues with a file."""

    success = True
    success = success and fix_known_bad_SEs(remotepath, verbose=verbose)
    success = success and fix_missing_files(remotepath, verbose=verbose)
    success = success and fix_checksum_errors(remotepath, verbose=verbose)
    return success
