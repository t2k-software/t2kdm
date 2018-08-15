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
from t2kdm.cache import Cache

# Long time cache to save the output of `is_dir`
long_cache = Cache(cache_time=600)
@long_cache.cached
def is_dir(*args, **kwargs):
    return t2kdm.backend.is_dir(*args, **kwargs)

def remote_iter_recursively(remotepath, regex=None):
    """Iter over remote paths recursively.

    If `regex` is given, only consider files/folders that match the reular expression.
    """

    if isinstance(regex, str):
        regex = re.compile(regex)

    if is_dir(remotepath):
        for entry in t2kdm.ls(remotepath):
            if regex is None or regex.search(entry.name):
                new_path = posixpath.join(remotepath, entry.name)
                for path in remote_iter_recursively(new_path, regex):
                    yield path
    else:
        yield remotepath

def check_checksums(remotepath):
    """Check if the checksums of all replicas are identical."""

    replicas = t2kdm.replicas(remotepath)
    checksum = t2kdm.checksum(replicas[0])

    if '?' in checksum:
        return False

    for rep in replicas[1:]:
        if t2kdm.checksum(rep) != checksum:
            return False

    return True

def check_replicas(remotepath, ses):
    """Check whether the file is replcated to the given SE(s)."""

    check_ses = []

    for se in ses:
        if isinstance(se, str):
            se = storage.get_SE(se)
            if se is None:
                raise backends.BackendException("Not a valid storage element: %s"%(se,))
        check_ses.append(se)

    for se in check_ses:
        if not se.has_replica(remotepath):
            return False

    return True
