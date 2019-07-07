"""Module for convenience functions that go beyond the basic backend capabilities."""

import posixpath
from copy import deepcopy
from six import print_
import os, sys, sh
import tempfile
from contextlib import contextmanager
import re
import hkdm as dm
from hkdm import backends
from hkdm import storage
from time import sleep

@contextmanager
def temp_dir():
    tempdir = tempfile.mkdtemp()
    try:
        yield tempdir
    finally:
        sh.rm('-r', tempdir)

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
    for i in range(3):
        # Try three times
        try:
            isdir = dm.is_dir(remotepath, cached=True) or (se is not None and dm.is_dir_se(remotepath, se, cached=True))
        except Exception as e:
            print_("Recursion failure! (%d)"%(i,))
            if ignore_exceptions:
                print_(e)
            else:
                raise
        else:
            # Break loop if no exception was raised (success)
            break
        sleep(10)
    else:
        # Return if loop was not broken
        return

    if isdir:
        for i in range(3):
            # Try three times
            try:
                if se is None:
                    entries = dm.iter_ls(remotepath)
                else:
                    entries = dm.iter_ls_se(remotepath, se)
            except Exception as e:
                print_("Recursion failure! (%d)"%(i,))
                if ignore_exceptions:
                    print_(e)
                else:
                    raise
                return
            else:
                # Break loop if no exception was raised (success)
                break
            sleep(10)
        else:
            # Return if loop was not broken
            return
        for entry in entries:
            if regex is None or regex.search(entry.name):
                new_path = posixpath.join(remotepath, entry.name)
                for path in remote_iter_recursively(new_path, regex, se=se, ignore_exceptions=ignore_exceptions):
                    yield path
    else:
        yield str(remotepath)

def check_checksums(remotepath, cached=False):
    """Check if the checksums of all replicas are identical."""

    replicas = dm.replicas(remotepath, cached=cached)
    checksum = dm.checksum(replicas[0], cached=cached)

    if '?' in checksum:
        return False

    for rep in replicas[1:]:
        if dm.checksum(rep, cached=cached) != checksum:
            return False

    return True

def check_replica_states(remotepath, cached=False):
    """Check if the state of all replicas."""

    replicas = dm.replicas(remotepath, cached=cached)

    for rep in replicas:
        if dm.state(rep, cached=cached) not in ['ONLINE', 'NEARLINE', 'ONLINE_AND_NEARLINE']:
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

    replicas = dm.replicas(remotepath)
    for replica in replicas:
        se = storage.get_SE(replica)
        if se is None:
            print_("Found replica on unknown storage element: "+replica)
            success = False
        elif se.broken:
            if verbose:
                print_("Found replica on bad storage element. Unregistering replica: "+replica)
            try:
                dm.backend.deregister(replica, remotepath, verbose=verbose)
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

    replicas = dm.replicas(remotepath)
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
                exists = dm.exists(rep)
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
                    dm.backend.deregister(replica, remotepath, verbose=verbose)
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
            dm.replicate(remotepath, se, verbose=verbose)
        except backends.BackendException():
            if verbose:
                print_("Failed to replicate File.")
            success = False

    return success

def _test_replica(replica, verbose=False):
    """Test whether a replica has the checksum it reports and whether it passes the gzip test."""

    with temp_dir() as tempdir:
        tempf = os.path.join(tempdir, 'temp.gz')
        if verbose:
            print_("Downloading and checking replica: "+replica)
        dm.backend._get(replica, tempf, verbose=verbose)

        remote_checksum = dm.checksum(replica)
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

def fix_checksum_errors(remotepath, verbose=False):
    """Fix replicas with differing checksums.

    This can only be done for files that can be checked for corruption.
    Otherwise there is no way to decide which file is actually the correct one.
    """

    replicas = dm.replicas(remotepath)
    checksums = [dm.checksum(r) for r in replicas]

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
            dm.remove(remotepath, SE, verbose=verbose)
        except:
            success = False

    for SE in bad_SEs:
        if verbose:
            print_("Re-replicating file on %s."%(SE.name,))
        try:
            dm.replicate(remotepath, SE, verbose=verbose)
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

def _bgstyle(size):
    return "background:linear-gradient(to left,#8888FF 0%%, #8888FF calc(100%% * %d / var(--maxsize)), #FFFFFF calc(100%% * %d / var(--maxsize)), #FFFFFF 100%%);"%(size,size)

def _number_chunks(number):
    number = str(number)
    n = len(number)
    while n > 0:
        x = n % 3
        if x == 0:
            x = 3
        yield number[0:x]
        number = number[x:]
        n = len(number)

def _format_number(number):
    return '<span style="margin-left:3pt"></span>'.join(_number_chunks(number))

def html_index(remotepath, localdir, recursive=False, topdir=False, verbose=False):
    """Generate a HTML index of the remote path in the local directory.

    Returns the sum of the file sizes in the directory.
    """

    if not os.path.isdir(localdir):
        raise IOError("No such directory.")

    if verbose:
        print_("Creating index for %s..."%(remotepath,))

    with temp_dir() as tempdir:
        index_name = os.path.join(tempdir, "index.html")
        size = 0
        maxsize = 1
        with open(index_name, 'wt') as f:
            f.write("<!DOCTYPE html><html><head><title>%s</title></head><body><h3>%s</h3><table>\n"%(remotepath,remotepath))
            f.write("<tr><th>size</th><th>modified</th><th>name</th></tr>\n")
            if topdir:
                # link to dir one level up
                f.write("<tr><td style=\"text-align:right;\">-</td><td>-</td><td><a href=\"../index.html\">../</a></td></tr>\n")
            for entry in dm.iter_ls(remotepath):
                path = posixpath.join(remotepath, entry.name)
                if dm.is_dir(path):
                    if recursive:
                        newdir = os.path.join(localdir, entry.name)
                        try:
                            os.mkdir(newdir)
                        except OSError:
                            # directory probably exists
                            pass
                        sub_size = html_index(path, newdir, recursive=True, topdir=True, verbose=verbose)
                        f.write("<tr><td style=\"text-align:right;%s\">%s</td><td>%s</td><td><a href=\"%s/index.html\">%s/</a></td></tr>\n"%(_bgstyle(sub_size), _format_number(sub_size), entry.modified, entry.name, entry.name))
                        size += sub_size
                        maxsize = max(maxsize, sub_size)
                    else:
                        f.write("<tr><td style=\"text-align:right;\">%s</td><td>%s</td><td>%s/</td></tr>\n"%(_format_number(entry.size), entry.modified, entry.name))
                else:
                    # Not a dir
                    f.write("<tr><td style=\"text-align:right;%s\">%s</td><td>%s</td><td>%s</td></tr>\n"%(_bgstyle(entry.size), _format_number(entry.size), entry.modified, entry.name))
                    if entry.size > 0:
                        size += entry.size
                        maxsize = max(maxsize, entry.size)
            f.write("</table><p>Total size: %s</p><style>:root{--maxsize: %d} td,th{padding-left:3pt; padding-right:3pt;}</style></body></html>\n"%(_format_number(size),maxsize))

        # Move file over
        sh.mv(index_name, localdir)

    return size
