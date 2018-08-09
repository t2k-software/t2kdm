"""Module for convenience functions that go beyond the basic backend capabilities."""

import posixpath
from copy import deepcopy
import t2kdm
from six import print_
import sys, sh
from contextlib import contextmanager
import re
from t2kdm.cache import Cache

# Long time cache to save the output of `is_dir`
long_cache = Cache(cache_time=600)
@long_cache.cached
def is_dir(*args, **kwargs):
    return t2kdm.backend.is_dir(*args, **kwargs)

def strip_output(output):
    """Turn the output of a command into an iterable.

    Returns one element per line, stripped of leading and trailing white
    spaces. Requires an iterable command output.
    """

    for line in output:
        line = line.strip()
        if len(line) > 0:
            yield line

class Replica(object):
    """Simple helper class that contains information of a replica."""
    def __init__(self, line):
        """Initialise from a line of output of `replicas -l`."""
        x = line.split()
        if len(x) > 1:
            # The full information
            self.SE_name = x[0]
            self.SE_type = x[1]
            self.status = x[2]
            self.checksum = x[3]
            self.path = x[4]
        else:
            # Just the replica url
            self.path = x[0]
            SE = t2kdm.storage.get_SE(x[0])
            if SE is not None:
                self.SE_name = SE.name
                self.SE_type = SE.type
            else:
                self.SE_name = 'UNKOWN'
                self.SE_type = '?'
            self.status = '?'
            self.checksum = '-' # Not a '?', this would trigger a checksum error

class ReplicaState(object):
    """Deal with the state of a file's replicas."""

    def __init__(self, remotepath, full=True):
        """Get the ReplicaState of a given remotepath.

        If `full` is `False`, only check what replicas exist, but no additional information.
        """

        self.replicas = []
        for line in strip_output(t2kdm.replicas(remotepath, long=full, _iter=True)):
            self.replicas.append(Replica(line))

    def get_file_categories(self):
        """Return a list of categories the file falls into.

        Supported categories are:

            all - all files are in this category
            checksum_error - do the checksums of the replicas differ?
            no_replica - There is no replica of this file at all
            SE_<SE_name> - all files replicated on SE <SE_name>

        """

        # List of the categories
        cat = []

        # Set of checksums
        checksums = set()

        # All files are in 'all'
        cat.append('all')

        # Add all SEs
        for rep in self.replicas:
            checksums.add(rep.checksum)
            cat.append('SE_%s'%(rep.SE_name))

        c = len(checksums)
        if c == 0:
            cat.append('no_replica')
        if c > 1 or '?' in checksums or u'?' in checksums:
            cat.append('checksum_error')

        return cat

class CheckResult(object):
    """Class to contain the results of a replica check.

    It contains sets of all checked files in a directory. Each set
    corresponds to a certain category, e.g. all checked files, files
    with checksum problems, files replicated to a specific SE, etc.

    It can also contain further CheckResults of subdirectories.
    """

    def __init__(self, path):
        """Initialise the CheckResult.

        It will have to be filled with actual results afterwards.
        """

        self.path = path

        # Dictionary of sets
        self.file_sets = {}

        # List of results of subdirectories
        self.subresults = []

    def add_file_to_set(self, f, s, dictionary=None):
        """Add a file to the specified set.

        If the set does not exist yet, create it.
        """

        if dictionary is None:
            d = self.file_sets

        if s not in d:
            self.file_sets[s] = set()

        d[s].add(f)

    def add(self, other):
        """Add files from other CheckResult to this one."""

        # Add files
        for s in other.file_sets:
            for f in other.file_sets[s]:
                self.add_file_to_set(f, s)

        # Add sub results
        self.subresults.extend(other.subresults)

    def add_subresult(self, check_result):
        """Add a CheckResult of a sub directory."""
        self.subresults.append(check_result)

    def collect_file_sets(self):
        """Return file sets with all files including subdirectories."""

        # Copy own sets so we do not modify them
        ret = deepcopy(self.file_sets)

        # Add all sets from sub-results
        for subresult in self.subresults:
            # Get collected sub-results
            col = subresult.collect_file_sets()
            # Go through all sets and add to return value
            for s in col:
                if s not in ret:
                    ret[s] = set()

                # Update the results
                ret[s] |= col[s]

        return ret

    @staticmethod
    def _report_checksum(sets, list_faulty=True):
        """Report faulty checksums."""
        try:
            faulty = sets['checksum_error']
        except KeyError:
            faulty = set()
        n_err = len(faulty)
        report = "%d files have faulty checksums.\n"%(n_err,)
        if list_faulty:
            for f in sorted(list(faulty)):
                report += " -> %s\n"%(f,)
        return report

    @staticmethod
    def _report_SE(sets, SE, list_faulty=True):
        """Report replication on given SE."""

        if SE not in t2kdm.storage.SE_by_name and SE != 'UNKNOWN':
            raise ValueError("Not a valid SE: %s"%(SE,))

        try:
            all = (sets['all'])
        except KeyError:
            all = set()
        try:
            replicated = sets['SE_'+SE]
        except KeyError:
            replicated = set()
        faulty = all - replicated
        n_rep = len(replicated)
        n_err = len(faulty)
        report = "%d files have been replicated on %s.\n"%(n_rep, SE)
        report += "%d files are missing from %s.\n"%(n_err, SE)
        if list_faulty:
            for f in sorted(list(faulty)):
                report += " -> %s\n"%(f,)
        return report

    def report(self, *args, **kwargs):
        """Create a report of the result.

        The arguments determine what is reported on and in what order.
        Valid report types:

            checksum - report files with checksum errors
            SE_<SE_name> - report files that are *not* replicated on the given SE

        If no report types are provided, the following reports are generated:

            checksum

        If the keyword argument `list_faulty` is `True` (default), all
        problematic files are listed. Otherwise only the number of faulty files
        is reported.
        """

        # Get total sets of all files in all directories
        sets = self.collect_file_sets()

        # Use standard list of reports if none are provided
        if len(args) == 0:
            args = ['checksum']
        list_faulty = kwargs.pop('list_faulty', True)

        # Print total number of files
        try:
            n_all = len(sets['all'])
        except KeyError:
            n_all = 0
        report = "Checked a total of %d files in %s.\n"%(n_all, self.path)

        # Add requested reports
        for arg in args:
            if arg == 'checksum':
                report += self._report_checksum(sets, list_faulty=list_faulty)
            elif arg.startswith('SE_'):
                report += self._report_SE(sets, arg[3:], list_faulty=list_faulty)
            else:
                raise ValueError("Unknown report type.""")

        return report

    def list_faulty(self, *args):
        """Return a list of faulty files, according to the provided checks (see `report`)."""
        faulty = set()
        for line in self.report(*args).split('\n'):
            if line.startswith(' -> '):
                faulty.add(line[4:])
        return sorted(list(faulty))

def check_replicas(remotepath, recursive=False, progress=False, quick=False):
    """Check the replicas of a given path.

    Returns a CheckResult.

    Arguments
    ---------

    recursive: bool/string. All files and sub-directories of a given path are replicated.
               If `recursive` is a string, it is treated as regular expression
               and only matching subfolders or files are replicated.
               Default: `False`
    progress: bool. Print out progress information on screen.
              Default: `False`
    quick: bool. Do not do the checksum test. Makes things go faster.
           Default `False`
    """

    # Need to differentiate between progress in base calla nd recursive calls
    if progress is True: # Base call, set progress to 2
        progress = 2
    elif progress == 2: # Recursive call, set progress to 1
        progress = 1
    elif progress is False: # No progress requested, set to 0
        progress = 0

    result = CheckResult(remotepath)

    # Do thing recursively if requested
    if isinstance(recursive, str):
        regex = re.compile(recursive)
        recur = True
    else:
        regex = None
        recur = recursive

    if is_dir(remotepath):
        # Go through the contents of the directory
        if progress == 2:
            print_("Checking files. This might take a while...")
        for f in strip_output(t2kdm.ls(remotepath, _iter=True)):
            if regex is not None and not regex.search(f):
                continue # Skip files/directories that do not match the regex
            newpath = posixpath.join(remotepath, f)
            if is_dir(newpath):
                # This is a directory
                if recur:
                    # Add subresult of checking subdirectory recursively
                    result.add_subresult(check_replicas(newpath, recursive=recursive, progress=progress, quick=quick))
            else:
                # Check file replicas and add to current result
                if progress > 0:
                    print_('.', end='')
                    sys.stdout.flush() # Make sure every dot is printed directly to screen
                result.add(check_replicas(newpath, quick=quick))
        if progress == 2:
            print_("\nDone.")
    else:
        # Handle single file
        rep_state = ReplicaState(remotepath, full=not quick)

        for category in rep_state.get_file_categories():
            result.add_file_to_set(remotepath, category)

    return result

def check(remotepath, checksum=False, se=[],
        recursive=False, nolist=False, list=None, **kwargs):
    """Check a remote path for problems with the replicas.

    Arguments
    ---------

    checksum: bool. Check whether the checksums of all replicas are identical.
              Default `False`
    se: list. List of SE names for which a replication report should be printed.
        Default []
    recursive: bool. Go through sub directories recursively.
               Default `False`
    nolist: bool. Do not list the faulty files in te report.
            Default `False`
    list: string. Store a list of problematic files in the given filename.
          Default `None`

    Behaves like (kinda) like an `sh` command.
    """

    tests = []
    if checksum:
        tests.append('checksum')
    for s in se:
        SE = t2kdm.storage.get_SE(s)
        if SE is None:
            raise sh.ErrorReturnCode_1('', '',
                    "Not a valid storage element: %s\n"%(s,))
        tests.append('SE_'+SE.name)

    if len(tests) == 0:
        raise sh.ErrorReturnCode_1('', '',
                "Nothing to report! You need to specify at least on check to be performed.\n"%(s,))

    # Test the files and get the result
    result = check_replicas(remotepath, recursive=recursive, progress=True, quick=not checksum)

    if list is not None:
        # Save the list of faulty files
        with open(list, 'wt') as f:
            for fault in result.list_faulty(*tests):
                f.write('%s\n'%(fault,))

    report = result.report(*tests, list_faulty=not nolist)
    it = kwargs.pop('_iter', False)
    if not it:
        # Just return the check report
        return report
    else:
        # We need to provide a generator that yields the lines of the putput
        return (line+'\n' for line in report.split('\n')[:-1]) # Last element of split is always empty string
