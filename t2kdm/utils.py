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
