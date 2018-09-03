"""Module for convenience functions that go beyond the basic backend capabilities."""

import posixpath
from copy import deepcopy
from six import print_
import sys, sh
from contextlib import contextmanager
import re
import t2kdm
from t2kdm import backends
from t2kdm import storage

def remote_iter_recursively(remotepath, regex=None):
    """Iter over remote paths recursively.

    If `regex` is given, only consider files/folders that match the reular expression.
    """

    if isinstance(regex, str):
        regex = re.compile(regex)

    if t2kdm.is_dir(remotepath):
        for entry in t2kdm.ls(remotepath):
            if regex is None or regex.search(entry.name):
                new_path = posixpath.join(remotepath, entry.name)
                for path in remote_iter_recursively(new_path, regex):
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
        if not se.has_replica(remotepath, cached=cached):
            return False

    return True

def fix_known_bad_SEs(remotepath, verbose=False):
    """Fix replicas on known bad storage elements.

    Unregisters all replicas on

        IN2P3-CC-disk

    """

    success = True

    se = storage.SE_by_name['IN2P3-CC-disk']
    replica = se.get_replica(remotepath)
    if replica is not None:
        if verbose:
            print_("Found replica on bad storage element. Unregistering replica: "+replica)
        try:
            t2kdm.backend.unregister(replica, remotepath, verbose=verbose)
        except backends.BackendException():
            if verbose:
                print_("Failed to unregister replica.")
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
        raise RuntimeError("There is not a single replica actually present!")

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
                    t2kdm.backend.unregister(replica, remotepath, verbose=verbose)
                except backends.BackendException():
                    if verbose:
                        print_("Failed to unregister replica.")
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

def fix_all(remotepath, verbose=False):
    """Try to automatically fix some common issues with a file."""

    success = True
    success = success and fix_known_bad_SEs(remotepath, verbose=verbose)
    success = success and fix_missing_files(remotepath, verbose=verbose)
    return success
