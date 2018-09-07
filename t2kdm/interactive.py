"""Interactive commands for use in scripts and CLI.

See 'commands' module for descriptions of the parameters.
"""

from six import print_
import re
import t2kdm
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
        list_file = kwargs.pop('list', None)
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
        if recursive is True:
            for path in utils.remote_iter_recursively(remotepath, regex):
                if verbose:
                    print_(self.iterating + " " + path)
                try:
                    ret = self.function(path, *args, **kwargs)
                except Exception as e:
                    print_(e)
                    bad += 1
                    if list_file is not None:
                        list_file.write(path + '\n')
                else:
                    if ret == 0:
                        good += 1
                    else:
                        bad += 1
                        if list_file is not None:
                            list_file.write(path + '\n')
            if verbose:
                print_("%s %d files. %d files failed."%(self.iterated, good, bad))
            if list_file is not None:
                list_file.close()
            if bad == 0:
                return 0
            else:
                return 1
        else:
            ret = self.function(remotepath, *args, **kwargs)
            if list_file is not None:
                if ret != 0:
                    list_file.write(remotepath + '\n')
                list_file.close()
            return ret

    def __call__(self, function):
        self.function = function
        return self.recursive_function

def ls(*args, **kwargs):
    """Print the contents of a directory on screen."""

    long = kwargs.pop('long', False)
    entries = t2kdm.ls(*args, **kwargs)
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

def replicas(*args, **kwargs):
    """Print the replicas of a file on screen."""

    checksum = kwargs.pop('checksum', False)
    state = kwargs.pop('state', False)
    name = kwargs.pop('name', False)
    reps = t2kdm.replicas(*args, **kwargs)
    for r in reps:
        if checksum:
            print_(t2kdm.checksum(r), end=' ')
        if state:
            print_(t2kdm.state(r), end=' ')
        if name:
            se = t2kdm.storage.get_SE(r)
            if se is None:
                print_('?', end=' ')
            else:
                print_(se.name, end=' ')
        print_(r)
    return 0

@_recursive("Replicating", "Replicated")
def replicate(remotepath, *args, **kwargs):
    """Replicate files to a storage element."""

    bringonline = kwargs.pop('bringonline', False)
    verbose = kwargs.pop('verbose', False)
    kwargs['verbose'] = verbose

    if bringonline:
        timeout = 2
    else:
        timeout = 60*60*6

    ret = t2kdm.replicate(remotepath, *args, bringonline_timeout=timeout, **kwargs)
    if ret is False:
        return 1
    else:
        return 0

@_recursive("Getting", "Downloaded")
def get(remotepath, *args, **kwargs):
    """Download files."""

    bringonline = kwargs.pop('bringonline', False)
    verbose = kwargs.pop('verbose', False)
    kwargs['verbose'] = verbose

    if bringonline:
        timeout = 2
    else:
        timeout = 60*60*6

    ret = t2kdm.get(remotepath, *args, bringonline_timeout=timeout, **kwargs)
    if ret is False:
        return 1
    else:
        return 0

def put(*args, **kwargs):
    """Upload a file to the grid."""
    ret = t2kdm.put(*args, **kwargs)
    if ret:
        return 0
    else:
        return 1

@_recursive("Removing", "Removed")
def remove(remotepath, *args, **kwargs):
    """Remove a file from a given SE."""
    verbose = kwargs.pop('verbose', False)
    kwargs['verbose'] = verbose

    ret = t2kdm.remove(remotepath, *args, **kwargs)
    if ret == True:
        return 0
    else:
        return 1

@_recursive("Checking", "No problems detected for")
def check(remotepath, *args, **kwargs):
    """Check if everything is alright with the files."""

    verbose = kwargs.pop('verbose', False)
    quiet = kwargs.pop('quiet', False)
    ses = kwargs.pop('se', [])
    checksum = kwargs.pop('checksum', False)

    if checksum == False and len(ses) == 0:
        raise InteractiveException("No check specified.")

    if t2kdm.is_dir(remotepath):
        raise InteractiveException("%s is a directory. Maybe you want to use the `--recursive` option?"%(remotepath,))

    if verbose and len(ses) > 0:
        print_("Checking replicas...")
    ret = t2kdm.check_replicas(remotepath, ses, cached=True)
    if not ret and not quiet:
        print_("%s is not replicated on all SEs!"%(remotepath))

    if checksum:
        if verbose:
            print_("Checking checksums...")
        chk = t2kdm.check_checksums(remotepath, cached=True)
        if not chk and not quiet:
            print_("%s has faulty checksums!"%(remotepath))
        ret = ret and chk

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
