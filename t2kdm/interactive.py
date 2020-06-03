"""Interactive commands for use in scripts and CLI.

See 'commands' module for descriptions of the parameters.
"""

from six import print_
from six.moves import map
import re
from multiprocess import Pool
import os, signal
import t2kdm as dm
from t2kdm import storage
from t2kdm import utils
from t2kdm import backends

class InteractiveException(Exception):
    """Exception to be raised for interactive errors, e.g. an illegal user argument."""
    pass

class _recursive(object):
    """Decorator to make a function work recursively."""

    def __init__(self, iterating="Iterating over", iterated="Succesfully iterated over"):
        self.iterating = iterating
        self.iterated = iterated
        self.function = None

    def recursive_function(self, remotepath, *args, **kwargs):
        """The recursive wrapper around the original function."""
        recursive = kwargs.pop('recursive', False)
        recursive_se = kwargs.pop('recursivese', None)
        if recursive_se is not None and recursive == False:
            recursive = True
        list_file = kwargs.pop('list', None)
        parallel = kwargs.pop('parallel', 1)
        if 'verbose' in kwargs:
            verbose = kwargs['verbose']
        else:
            verbose = False

        if isinstance(recursive, str):
            regex = re.compile(recursive)
            recursive = True
        else:
            regex = None

        if list_file is not None:
            list_file = open(list_file, 'wt')

        good = 0
        bad = 0
        def is_good(path):
            if verbose:
                print_(self.iterating + " " + path)
            try:
                ret = self.function(path, *args, **kwargs)
            except Exception as e:
                print_(self.iterating + " " + path + " failed.")
                print_(e)
                return False, path
            else:
                if ret == 0:
                    return True, path
                else:
                    return False, path

        if recursive is True:
            if parallel > 1:
                # Deal with signal weirdness when using a Pool
                # Otherwise we won't be able to kill things with CTRL-C
                def abort(*args, **kwargs):
                    print_("%d Aborting!"%(os.getpid(),))
                    raise Exception("%d Aborting!"%(os.getpid(),))
                orig_sigint = signal.getsignal(signal.SIGINT)
                orig_sigterm = signal.getsignal(signal.SIGTERM)
                signal.signal(signal.SIGINT, abort)
                signal.signal(signal.SIGTERM, abort)
                p = Pool(parallel)
                mapper = p.imap
            else:
                mapper = map

            try:
                for g, path in mapper(is_good, utils.remote_iter_recursively(remotepath, regex, se=recursive_se, ignore_exceptions=True)):
                    if g:
                        good += 1
                    else:
                        bad += 1
                        if list_file is not None:
                            list_file.write(path + '\n')
            except Exception:
                if parallel > 1:
                    # Kill all child processes
                    for proc in p._pool:
                        os.kill(proc.pid, signal.SIGTERM)
                raise
            finally:
                if parallel > 1:
                    # Clean up processing pool
                    p.terminate()
                    p.join()
                    del p
                    # Resetting signal handlers
                    signal.signal(signal.SIGINT, orig_sigint)
                    signal.signal(signal.SIGTERM, orig_sigterm)

            if verbose:
                print_("%s %d files. %d files failed."%(self.iterated, good, bad))
            if list_file is not None:
                list_file.close()
            if bad == 0:
                return 0
            else:
                return 1
        else:
            g, path = is_good(remotepath)
            if list_file is not None:
                if not g:
                    list_file.write(remotepath + '\n')
                list_file.close()
            if g:
                return 0
            else:
                return 1

    def __call__(self, function):
        self.function = function
        return self.recursive_function

def _check_path(remotepath):
    """Make sure the remotepath is absolute."""
    if not remotepath.startswith('/'):
        raise InteractiveException("Remote path must be absolute!")

def ls(remotepath, *args, **kwargs):
    """Print the contents of a directory on screen."""
    _check_path(remotepath)

    long = kwargs.pop('long', False)
    se = kwargs.pop('se', None)
    if se is None:
        entries = dm.iter_ls(remotepath, *args, **kwargs)
    else:
        entries = dm.iter_ls_se(remotepath, *args, se=se, **kwargs)
    if long:
        # Detailed listing
        for e in entries:
            print_("{mode:<11} {links:4d} {uid:5} {gid:5} {size:13d} {modified:>12} {name}".format(
                name = e.name,
                mode = e.mode,
                links = e.links,
                uid = e.uid,
                gid = e.gid,
                size = e.size,
                modified = e.modified))
    else:
        # Just the names
        for e in entries:
            print_(e.name)
    return 0

def replicas(remotepath, *args, **kwargs):
    """Print the replicas of a file on screen."""
    _check_path(remotepath)

    checksum = kwargs.pop('checksum', False)
    state = kwargs.pop('state', False)
    name = kwargs.pop('name', False)
    distance = kwargs.pop('distance', False)
    if distance:
        if isinstance(distance, str):
            reps = [x[0] for x in dm.iter_file_sources(remotepath, destination=distance, tape=True)]
        else:
            reps = [x[0] for x in dm.iter_file_sources(remotepath, tape=True)]
    else:
        reps = dm.replicas(remotepath, *args, **kwargs)
    for r in reps:
        if checksum:
            try:
                chk = dm.checksum(r)
            except Exception as e:
                chk = str(e)
            print_(chk, end=' ')
        if state:
            try:
                stat = dm.state(r)
            except Exception as e:
                stat = str(e)
            print_(stat, end=' ')
        if name:
            se = dm.storage.get_SE(r)
            if se is None:
                print_('?', end=' ')
            else:
                print_(se.name, end=' ')
        print_(r)
    return 0

@_recursive("Replicating", "Replicated")
def replicate(remotepath, *args, **kwargs):
    """Replicate files to a storage element."""
    _check_path(remotepath)

    bringonline = kwargs.pop('bringonline', False)

    if bringonline:
        timeout = 2
    else:
        timeout = 60*60*6

    ret = dm.replicate(remotepath, *args, bringonline_timeout=timeout, **kwargs)
    if ret is False:
        return 1
    else:
        return 0

@_recursive("Getting", "Downloaded")
def get(remotepath, *args, **kwargs):
    """Download files."""
    _check_path(remotepath)

    bringonline = kwargs.pop('bringonline', False)

    if bringonline:
        timeout = 2
    else:
        timeout = 60*60*6

    ret = dm.get(remotepath, *args, bringonline_timeout=timeout, **kwargs)
    if ret is False:
        return 1
    else:
        return 0

def put(localpath, remotepath, *args, **kwargs):
    """Upload a file to the grid."""
    _check_path(remotepath)

    ret = dm.put(localpath, remotepath, *args, **kwargs)
    if ret:
        return 0
    else:
        return 1

@_recursive("Removing", "Removed")
def remove(remotepath, *args, **kwargs):
    """Remove a file from a given SE."""
    _check_path(remotepath)

    ret = dm.remove(remotepath, *args, **kwargs)
    if ret == True:
        return 0
    else:
        return 1

def rmdir(remotepath, *args, **kwargs):
    """Remove a directory from the catalogue."""
    _check_path(remotepath)

    ret = dm.rmdir(remotepath, *args, **kwargs)
    if ret == True:
        return 0
    else:
        return 1

@_recursive("Moving", "Moved")
def move(oldremotepath, newremotepath, *args, **kwargs):
    """Move a file to a new position."""
    _check_path(oldremotepath)
    _check_path(newremotepath)

    ret = dm.move(oldremotepath, newremotepath, *args, **kwargs)
    if ret == True:
        return 0
    else:
        return 1

@_recursive("Renaming", "Renamed")
def rename(remotepath, regex_from, regex_to, *args, **kwargs):
    """Rename a file."""
    _check_path(remotepath)

    ret = dm.rename(remotepath, regex_from, regex_to, *args, **kwargs)
    if ret == True:
        return 0
    else:
        return 1

@_recursive("Checking", "No problems detected for")
def check(remotepath, *args, **kwargs):
    """Check if everything is alright with the files."""
    _check_path(remotepath)

    verbose = kwargs.pop('verbose', False)
    quiet = kwargs.pop('quiet', False)
    ses = kwargs.pop('se', [])
    checksum = kwargs.pop('checksum', False)
    states = kwargs.pop('states', False)

    if checksum == False and len(ses) == 0 and states == False:
        raise InteractiveException("No check specified.")

    if dm.is_dir(remotepath, cached=True):
        raise InteractiveException("%s is a directory. Maybe you want to use the `--recursive` option?"%(remotepath,))

    ret = True

    if len(ses) > 0:
        if verbose:
            print_("Checking replicas...")
        ret = ret and dm.check_replicas(remotepath, ses, cached=True)
        if not ret and not quiet:
            print_("%s is not replicated on all SEs!"%(remotepath))

    if checksum:
        if verbose:
            print_("Checking checksums...")
        chk = dm.check_checksums(remotepath, cached=True)
        if not chk and not quiet:
            print_("%s has faulty checksums!"%(remotepath))
        ret = ret and chk

    if states:
        if verbose:
            print_("Checking replica states...")
        stat = dm.check_replica_states(remotepath, cached=True)
        if not stat and not quiet:
            print_("%s has faulty replica states!"%(remotepath))
        ret = ret and stat

    if ret == True:
        return 0
    else:
        return 1

@_recursive("Fixing", "Fixed")
def fix(remotepath, **kwargs):
    ret = utils.fix_all(remotepath, **kwargs)
    if ret:
        return 0
    else:
        return 1

def print_storage_elements():
    """Print all available storage elments on screen."""

    for se in storage.SEs:
        print_(se)
    return 0

def html_index(remotepath, localpath, **kwargs):
    """Create a static html index of the catalogue."""
    ret = utils.html_index(remotepath, localpath, **kwargs)
    if ret >= 0: # html_index returns the size of directory contents
        return 0
    else:
        return 1
