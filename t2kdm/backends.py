import sh
import itertools
import posixpath
import os
from t2kdm import storage
import re

class CommandChain(object):
    """Class that executes function calls in sequence.

    It expects the functions to return iterables and will return an iterable when called.
    """

    def __init__(self):
        """Initialise CommandChain.

        Actual commands have to be added with `add`.
        """

        self.commands = []
        self.args = []
        self.kwargs = []

    def add(self, cmd, *args, **kwargs):
        """Add a command o the chain."""

        self.commands.append(cmd)
        self.args.append(args)
        self.kwargs.append(kwargs)

    def __call__(self):
        """Yield from all commands in turn."""
        for c, a, k in zip(self.commands, self.args, self.kwargs):
            try:
                for line in c(*a, **k):
                    yield line
            except sh.ErrorReturnCode as e:
                print_(e.stderr, file=sys.stderr, end='')
            except sh.SignalException_SIGSEGV as e:
                print_(e.stderr, file=sys.stderr, end='')

class GridBackend(object):
    """Class that handles the actual work on the grid.

    This is just a base class that other classes must inherit from.

    The convention for remote paths is that public methods expect "t2k paths",
    i.e. paths within the t2k grid directory, omitting the common prefix.
    Internal methods, i.e. the ones beginning with an underscore '_',
    expect the full path to be passed to them.
    """

    def __init__(self, **kwargs):
        """Initialise backend.

        Accepts the follwoing keyword arguments:

        basedir: String. Default: '/t2k.org'
            Sets the base directory of the backend.
            All paths are specified relative to that position.
        """

        self.basedir = kwargs.pop('basedir', '/t2k.org')
        if len(kwargs) > 0:
            raise TypeError("Invalid keyword arguments: %s"%(list(kwargs.keys),))

    def should_raise(self, code=1, **kwargs):
        """Analyse kwargs and decide whether an exception should be raised in case of an error."""

        ok = kwargs.pop('_ok_code', [])
        if code in ok:
            return False
        else:
            return True

    def error(self, message, **kwargs):
        """Handle errors by either raising an Exception or just printing the error message."""

        if self.should_raise(code=1, **kwargs):
            raise sh.ErrorReturnCode_1('', '', message)
        else:
            return self._iterable_output_from_text(message, **kwargs)

    def full_path(self, relpath):
        """Prepend the base dir to a path."""
        return posixpath.normpath(self.basedir + relpath)

    def _ls(self, remotepath, **kwargs):
        raise NotImplementedError()

    def ls(self, remotepath, **kwargs):
        """List contents of a remote logical path.

        Supported keyword arguments:

        long: Bool. Default: False
            Print a longer, more detailed listing.
        directory: Bool. Default: False
            List directory entries instead of contents.
        """
        _path = self.full_path(remotepath)
        return self._ls(_path, **kwargs)

    def _is_dir(self, remotepath):
        """Is the remote path a directory?"""
        return str(self._ls(remotepath, long=True, directory=True)).strip()[0] == 'd'

    def is_dir(self, remotepath):
        """Is the remote path a directory?"""
        return self._is_dir(self.full_path(remotepath))

    def _replica_state(self, storagepath, **kwargs):
        """Internal method to get the state of a replica, e.g. 'ONLINE'."""
        raise NotImplementedError()

    def _replica_checksum(self, storagepath, **kwargs):
        """Internal method to get the checksum of a replica."""
        raise NotImplementedError()

    def _replicas(self, remotepath, **kwargs):
        raise NotImplementedError()

    def _add_replica_info(self, rep):
        SE = storage.get_SE_by_path(rep)
        if SE is not None:
            return "%-32s %-4s %-8s %8.8s %s"%(SE.name, SE.type, self._replica_state(rep), self._replica_checksum(rep), rep)
        else:
            return "%-32s %-4s %-8s %8.8s %s"%('UNKNOWN', '?', self._replica_state(rep), self._replica_checksum(rep), rep)

    @staticmethod
    def _iterable_output_from_text(text, **kwargs):
        """Turn a block of text into an iterable if necessary."""
        it = kwargs.pop('_iter', False)
        if it == False:
            # Nothing to do here
            return text
        else:
            # Split by lines and return iterable
            lines = text.split('\n')
            if text[-1] == '\n':
                # Remove last empty string from split
                lines = lines[:-1]
            return (line+'\n' for line in lines)

    @staticmethod
    def _iterable_output_from_iterable(iterable, **kwargs):
        """Turn an iterable into a block of text if necessary."""
        it = kwargs.pop('_iter', False)
        if it == True:
            # Nothing to do here
            return iterable
        else:
            # Concatenate all lines
            text = ""
            for line in iterable:
                text += line
            return text

    def replicas(self, remotepath, **kwargs):
        """List replicas of a remote logical path.

        Supported keyword arguments:

        long: Bool. Default: False
            Print a longer, more detailed listing.
        """
        _path = self.full_path(remotepath)
        l = kwargs.pop('long', False)
        if l:
            # Parse each line and add additional information
            it = kwargs.pop('_iter', False)
            kwargs['_iter'] = True
            return self._iterable_output_from_iterable(
                    (self._add_replica_info(line) for line in self._replicas(_path, **kwargs)),
                    _iter=it)
        else:
            return self._replicas(_path, **kwargs)

    def _bringonline(self, storagepath, timeout, **kwargs):
        raise NotImplementedError()

    def bringonline(self, replica, **kwargs):
        """Make sure the given replica is online"""

        # We need to do multiple tries with short timeouts,
        # because it seems like the bringonline commands do not notice when they succeed.
        timeout = 10
        tries = 1440
        for i in range(tries):
            try:
                self._bringonline(replica, timeout)
            except sh.ErrorReturnCode:
                # Did not work
                # Try again
                continue
            except sh.SignalException_SIGSEGV:
                # Did not work
                # Try again
                continue
            else:
                # It worked
                # exit loop
                break
        else:
            # Never reached a break
            # It did not work so we raise an error
            return self.error("Could not bring replica online.\n", **kwargs)

        return self._iterable_output_from_text("Replica is online.\n", **kwargs)

    def _replicate(self, source_storagepath, destination_storagepath, remotepath, **kwargs):
        raise NotImplementedError()

    def replicate(self, remotepath, destination, source=None, tape=False, recursive=False, **kwargs):
        """Replicate the file to the specified storage element.

        If no source storage elment is provided, the closest replica is chosen.
        If `tape` is `True`, tape SEs are considered when choosing the closest one.
        If `recursive` is `True`, all files and sub-directories of a given path are replicated.
        if `recursive` is a string, it is treated as regular expression
        and only matching subfolders or files are replicated.
        """

        # Do thing recursively if requested
        if isinstance(recursive, str):
            regex = re.compile(recursive)
            recursive = True
        else:
            regex = None

        # Chain commands so everything is returned like a single sh Command
        it = kwargs.pop('_iter', False)
        kwargs['_iter'] = True # Need to iterate to make command chaining possible
        chain = CommandChain()
        # Print out what we are about to do, if running recursively
        if recursive:
            chain.add(self._iterable_output_from_text, "Replicating %s\n"%(remotepath,), **kwargs)

        _path = self.full_path(remotepath)
        if recursive and self._is_dir(_path):
            # Go through the contents of the directory recursively
            newpaths = []
            for element in self.ls(remotepath, _iter=True):
                element = element.strip()
                if regex is None or regex.search(element):
                    newpaths.append(posixpath.join(remotepath, element))
            def outputs(paths):
                for path in paths:
                    # Ignore errors and segfaults when running recursively
                    ok = kwargs.pop('_ok_code', None)
                    ex = kwargs.pop('_bg_exc', None)
                    try:
                        yield self.replicate(path, destination, source, tape, recursive,
                                _ok_code=list(range(-255,256)), _bg_exc=False, **kwargs)
                    except sh.ErrorReturnCode:
                        pass
                    except sh.SignalException_SIGSEGV:
                        pass
                    if ok is not None:
                        kwargs['_ok_code'] = ok
                    if ex is not None:
                        kwargs['_bg_exc'] = ex
            iterable = itertools.chain.from_iterable(outputs(newpaths))
            chain.add(self._iterable_output_from_iterable, iterable, _iter=it)
            return self._iterable_output_from_iterable(chain(), _iter=it)

        # Get destination SE and check if file is already present
        dst = storage.get_SE(destination)
        if dst is None:
            return self.error("Could not find storage element %s.\n"%(destination,), **kwargs)

        if dst.has_replica(remotepath):
            # Replica already at destination, nothing to do here
            return self._iterable_output_from_text(
                    "Replica of %s already present at destination storage element %s.\n"%(remotepath, dst.name,), **kwargs)

        # Get source SE
        if source is None:
            src = dst.get_closest_SE(remotepath, tape=tape)
            if src is None:
                return self.error("Could not find valid storage element with replica of %s.\n"%(remotepath,), **kwargs)
        else:
            src = storage.get_SE(source)
            if src is None:
                return self.error("Could not find storage element %s.\n"%(source,), **kwargs)

            if not src.has_replica(remotepath):
                # Replica not present at source, throw error
                return self.error("%s\nNo replica present at source storage element %s\n"%(remotepath, src.name,), **kwargs)

        source_path = src.get_replica(remotepath)
        destination_path = dst.get_storage_path(remotepath)
        chain.add(self._iterable_output_from_text, "Copying %s to %s\n"%(source_path, destination_path), **kwargs)

        if src.type == 'tape':
            chain.add(self._iterable_output_from_text, "Bringing online %s\n"%(source_path,), **kwargs)
            chain.add(self.bringonline, source_path, **kwargs)
        chain.add(self._replicate, source_path, destination_path, _path, **kwargs)
        return self._iterable_output_from_iterable(chain(), _iter=it)

    def _get(self, storagepath, localpath, **kwargs):
        raise NotImplementedError()

    def get(self, remotepath, localpath, source=None, tape=False, recursive=False, force=False, **kwargs):
        """Download a file from the grid.

        If no source storage elment is provided, the closest replica is chosen.
        If `tape` is True, tape SEs are considered when choosing the closest one.
        If `recursive` is `True`, all files and sub-directories of a given path are replicated.
        If `recursive` is a string, it is treated as regular expression
        and only matching subfolders or files are replicated.
        If `force` is `True`, local files will be overwritten.
        """

        # Do thing recursively if requested
        if isinstance(recursive, str):
            regex = re.compile(recursive)
            recursive = True
        else:
            regex = None
        _path = self.full_path(remotepath)
        if recursive and self._is_dir(_path):
            # Go through the contents of the directory recursively
            it = kwargs.pop('_iter', False)
            newpaths = []
            for element in self.ls(remotepath, _iter=True):
                element = element.strip()
                if regex is None or regex.search(element):
                    _remote = posixpath.join(remotepath, element)
                    _local = os.path.join(localpath, element)
                    newpaths.append( (_remote, _local) )
            def outputs(paths):
                for rpath, lpath in paths:
                    # Ignore errors and segfaults when running recursively
                    ok = kwargs.pop('_ok_code', None)
                    ex = kwargs.pop('_bg_exc', None)
                    try:
                        yield self.get(rpath, lpath, source, tape, recursive, force,
                                _iter=True, _ok_code=list(range(-255,256)), _bg_exc=False, **kwargs)
                    except sh.ErrorReturnCode:
                        pass
                    except sh.SignalException_SIGSEGV:
                        pass
                    if ok is not None:
                        kwargs['_ok_code'] = ok
                    if ex is not None:
                        kwargs['_bg_exc'] = ex
            iterable = itertools.chain.from_iterable(outputs(newpaths))
            return self._iterable_output_from_iterable(iterable, _iter=it)

        # Do not overwrite files unless explicitly told to
        if os.path.isfile(localpath) and not force:
            if recursive == False:
                # Raise for a single file
                return self.error("File does already exist: %s.\n"%(localpath,))
            else:
                # Print status for recursive
                return self._iterable_output_from_text(
                        "File does already exist: %s.\n"%(localpath,), **kwargs)

        if source is None:
            # Get closest SE
            SE = storage.get_closest_SE(remotepath, tape=tape)
            if SE is None:
                return self.error("Could not find valid storage element with replica of %s.\n"%(remotepath,), **kwargs)
        else:
            # Use the provided source
            SE = storage.get_SE(source)
            if SE is None:
                return self.error("Could not find storage element %s.\n"%(source,), **kwargs)

            if not SE.has_replica(remotepath):
                # Replica not present at source, throw error
                return self.error("%s\nNo replica present at source storage element %s\n"%(remotepath, SE.name,), **kwargs)

        # Get the source replica
        replica = SE.get_replica(remotepath)

        # Append the basename to the localpath if it is a directory
        if os.path.isdir(localpath):
            localpath = os.path.join(localpath, posixpath.basename(remotepath))

        # Chain commands for bringing the file online and replicating it
        it = kwargs.pop('_iter', False)
        kwargs['_iter'] = True # Need to iterate to make command chaining possible
        chain = CommandChain()
        if SE.type == 'tape':
            chain.add(self._iterable_output_from_text, "Bringing online %s\n"%(replica,), **kwargs)
            chain.add(self.bringonline, replica, **kwargs)
        chain.add(self._get, replica, localpath, **kwargs)
        return self._iterable_output_from_iterable(chain(), _iter=it)

    def _put(self, localpath, storagepath, remotepath, **kwargs):
        raise NotImplementedError()

    def put(self, localpath, remotepath, destination=None, tape=False, **kwargs):
        """Upload and register a file.

        If no destination storage element is provided, the closest one will be chosen.
        """

        # Split the local path in dir and file
        path, base = posixpath.split(localpath)

        # If the remotepath ends with '/', append the filename to it
        if remotepath.endswith('/'):
            remotepath += base

        # Get the destination
        if destination is None:
            # Get closest SE
            SE = storage.get_closest_SE(tape=tape)
            if SE is None:
                return self.error("Could not find valid storage element\n", **kwargs)
        else:
            # Use the provided destination
            SE = storage.get_SE(destination)
            if SE is None:
                return self.error("Could not find storage element %s.\n"%(destination,), **kwargs)

        # Get the storage path
        surl = SE.get_storage_path(remotepath)

        # Upload and register the file
        _path = self.full_path(remotepath)
        return self._put(localpath, surl, _path, **kwargs)

    def _remove(self, storagepath, remotepath, last=False, **kwargs):
        """Remove the given replica and unregister it from the remotepath.

        If `last` is `True`, this replica is the last and the
        lfc entry should be removed as well.
        """
        raise NotImplementedError()

    def remove(self, remotepath, destination, recursive=False, final=False, **kwargs):
        """Remove the replica of a file from a storage element.

        If `recursive` is `True`, all files and sub-directories of a given path are removed.
        if `recursive` is a string, it is treated as regular expression
        and only matching subfolders or files are replicated.

        This command will refuse to remove the last replica of a file
        unless the `final` argument is `True`!
        """

        # Do thing recursively if requested
        if isinstance(recursive, str):
            regex = re.compile(recursive)
            recursive = True
        else:
            regex = None
        _path = self.full_path(remotepath)
        if recursive and self._is_dir(_path):
            # Go through the contents of the directory recursively
            it = kwargs.pop('_iter', False)
            newpaths = []
            for element in self.ls(remotepath, _iter=True):
                element = element.strip()
                if regex is None or regex.search(element):
                    newpaths.append(posixpath.join(remotepath, element))
            def outputs(paths):
                for path in paths:
                    # Ignore errors and segfaults when running recursively
                    ok = kwargs.pop('_ok_code', None)
                    ex = kwargs.pop('_bg_exc', None)
                    try:
                        yield self.remove(path, destination, recursive=recursive,
                                _iter=True, _ok_code=list(range(-255,256)), _bg_exc=False, **kwargs)
                    except sh.ErrorReturnCode:
                        pass
                    except sh.SignalException_SIGSEGV:
                        pass
                    if ok is not None:
                        kwargs['_ok_code'] = ok
                    if ex is not None:
                        kwargs['_bg_exc'] = ex
            iterable = itertools.chain.from_iterable(outputs(newpaths))
            return self._iterable_output_from_iterable(iterable, _iter=it)

        # Get destination SE and check if file is already not present
        dst = storage.get_SE(destination)
        if dst is None:
            return self.error("Could not find storage element %s.\n"%(destination,), **kwargs)

        if not dst.has_replica(remotepath):
            # Replica already not present at destination, nothing to do here
            return self._iterable_output_from_text(
                    "%s\nReplica not present at destination storage element %s.\n"%(remotepath, dst.name,), **kwargs)

        # Check how many replicas there are
        # If it is only one, refuse to delete it
        nrep = self.replicas(remotepath).count('\n') # count lines
        if not final and nrep <= 1:
            return self.error("Only one replica of file left! Aborting.\n", **kwargs)

        destination_path = dst.get_replica(remotepath)

        return self._remove(destination_path, _path, last=(nrep<=1), **kwargs)

class LCGBackend(GridBackend):
    """Grid backend using the LCG command line tools `lfc-*` and `lcg-*`."""

    def __init__(self, **kwargs):
        # LFC paths alway put a '/grid' as highest level directory.
        # Let us not expose that to the user.
        kwargs['basedir'] = '/grid'+kwargs.pop('basedir', '/t2k.org')
        GridBackend.__init__(self, **kwargs)

        #self._proxy_init_cmd = sh.Command('voms-proxy-init')
        self._ls_cmd = sh.Command('lfc-ls')
        self._replicas_cmd = sh.Command('lcg-lr')
        self._replica_state_cmd = sh.Command('lcg-ls')
        self._replica_checksum_cmd = sh.Command('lcg-get-checksum')
        self._bringonline_cmd = sh.Command('lcg-bringonline')
        self._replicate_cmd = sh.Command('lcg-rep')
        self._cp_cmd = sh.Command('lcg-cp')
        self._cr_cmd = sh.Command('lcg-cr')
        self._del_cmd = sh.Command('lcg-del')

    def _ls(self, remotepath, **kwargs):
        # Translate keyword arguments
        l = kwargs.pop('long', False)
        d = kwargs.pop('directory', False)
        args = []
        if l:
            args.append('-l')
        if -d:
            args.append('-d')
        args.append(remotepath)

        return self._ls_cmd(*args, **kwargs)

    def _replicas(self, remotepath, **kwargs):
        return(self._replicas_cmd('lfn:'+remotepath, **kwargs))

    def _replica_state(self, storagepath, **kwargs):
        _path = storagepath.strip()
        it = kwargs.pop('_iter', None)
        try:
            listing = self._replica_state_cmd('-l', _path, **kwargs)
        except sh.ErrorReturnCode:
            listing = '- - - - - ?'
        except sh.SignalException_SIGSEGV:
            listing = '- - - - - ?'
        return listing.split()[5]

    def _replica_checksum(self, storagepath, **kwargs):
        _path = storagepath.strip()
        it = kwargs.pop('_iter', None)
        try:
            listing = self._replica_checksum_cmd(_path, **kwargs)
        except sh.ErrorReturnCode:
            listing = '? -'
        except sh.SignalException_SIGSEGV:
            listing = '? -'
        try:
            checksum = listing.split()[0]
        except IndexError:
            # Something weird happened
            checksum = '?'
        return checksum

    @staticmethod
    def _ignore_identical_lines(iterable, **kwargs):
        last_line = None
        for line in iterable:
            if line == last_line:
                continue
            else:
                last_line = line
                yield line

    def _bringonline(self, storagepath, timeout, **kwargs):
        kwargs['_err_to_out'] = True # Verbose output is on stderr
        kwargs.pop('_err', None) # Cannot specify _err and _err_ro_out at same time

        # Get original command output
        return self._bringonline_cmd('-v', '--bdii-timeout', timeout, '--srm-timeout', timeout, '--sendreceive-timeout', timeout, '--connect-timeout', timeout, storagepath, **kwargs)

    def _replicate(self, source_storagepath, destination_storagepath, remotepath, **kwargs):
        kwargs['_err_to_out'] = True # Verbose output is on stderr
        kwargs.pop('_err', None) # Cannot specify _err and _err_ro_out at same time
        it = kwargs.pop('_iter', False) # should the out[put be an iterable?
        kwargs['_iter'] = True # Need iterable to ignore identical lines

        # Get original command output
        iterable = self._replicate_cmd('-v', '--sendreceive-timeout', 14400, '--checksum', '-d', destination_storagepath, source_storagepath, **kwargs)

        # Ignore lines that are identical to the previous
        iterable = self._ignore_identical_lines(iterable)

        # return requested kind of output
        return self._iterable_output_from_iterable(iterable, _iter=it)

    def _get(self, storagepath, localpath, **kwargs):
        kwargs['_err_to_out'] = True # Verbose output is on stderr
        kwargs.pop('_err', None) # Cannot specify _err and _err_ro_out at same time
        it = kwargs.pop('_iter', False) # should the out[put be an iterable?
        kwargs['_iter'] = True # Need iterable to ignore identical lines

        # Get original command output
        iterable = self._cp_cmd('-v', '--sendreceive-timeout', 14400, '--checksum', storagepath, localpath, **kwargs)

        # Ignore lines that are identical to the previous
        iterable = self._ignore_identical_lines(iterable)

        # return requested kind of output
        return self._iterable_output_from_iterable(iterable, _iter=it)

    def _put(self, localpath, storagepath, remotepath, **kwargs):
        kwargs['_err_to_out'] = True # Verbose output is on stderr
        kwargs.pop('_err', None) # Cannot specify _err and _err_ro_out at same time
        it = kwargs.pop('_iter', False) # should the out[put be an iterable?
        kwargs['_iter'] = True # Need iterable to ignore identical lines

        # Get original command output
        iterable = self._cr_cmd('-v', '--sendreceive-timeout', 14400, '--checksum', '-d', storagepath, '-l', remotepath, localpath, **kwargs)

        # Ignore lines that are identical to the previous
        iterable = self._ignore_identical_lines(iterable)

        # return requested kind of output
        return self._iterable_output_from_iterable(iterable, _iter=it)

    def _remove(self, storagepath, remotepath, last=False, **kwargs):
        kwargs['_err_to_out'] = True # Verbose output is on stderr
        kwargs.pop('_err', None) # Cannot specify _err and _err_ro_out at same time
        it = kwargs.pop('_iter', False) # should the output be an iterable?
        kwargs['_iter'] = True # Need iterable to ignore identical lines

        # Get original command output
        iterable = self._del_cmd('-v', storagepath, **kwargs)

        # Ignore lines that are identical to the previous
        iterable = self._ignore_identical_lines(iterable)

        # return requested kind of output
        return self._iterable_output_from_iterable(iterable, _iter=it)

class GFALBackend(GridBackend):
    """Grid backend using the GFAL command line tools `gfal-*`."""

    def __init__(self, **kwargs):
        # lfn paths alway need a '/grid' as highest level directory.
        # Let us not expose that to the user.
        kwargs['basedir'] = '/grid'+kwargs.pop('basedir', '/t2k.org')
        GridBackend.__init__(self, **kwargs)

        #self._proxy_init_cmd = sh.Command('voms-proxy-init')
        self._ls_cmd = sh.Command('gfal-ls').bake(color='never')
        self._replicas_cmd = sh.Command('gfal-xattr')
        self._replica_checksum_cmd = sh.Command('gfal-sum')
        self._bringonline_cmd = sh.Command('gfal-legacy-bringonline')
        self._cp_cmd = sh.Command('gfal-copy')
        self._register_cmd = sh.Command('gfal-legacy-register')
        self._unregister_cmd = sh.Command('gfal-legacy-unregister')
        self._del_cmd = sh.Command('gfal-rm')

    def _ls(self, remotepath, **kwargs):
        # Translate keyword arguments
        l = kwargs.pop('long', False)
        d = kwargs.pop('directory', False)
        args = []
        if l:
            args.append('-l')
        if -d:
            args.append('-d')
        args.append('lfn:'+remotepath)

        return self._ls_cmd(*args, **kwargs)

    def _replicas(self, remotepath, **kwargs):
        return(self._replicas_cmd('lfn:'+remotepath, 'user.replicas', **kwargs))

    def _replica_state(self, storagepath, **kwargs):
        _path = storagepath.strip()
        try:
            state = self._replicas_cmd(_path, 'user.status', **kwargs).strip()
        except sh.ErrorReturnCode:
            state = '?'
        except sh.SignalException_SIGSEGV:
            state = '?'
        return state

    def _replica_checksum(self, storagepath, **kwargs):
        _path = storagepath.strip()
        try:
            checksum = self._replica_checksum_cmd(_path, 'ADLER32', **kwargs).split()[1]
        except sh.ErrorReturnCode:
            checksum = '?'
        except sh.SignalException_SIGSEGV:
            checksum = '?'
        except IndexError:
            checksum = '?'
        return checksum

    def _bringonline(self, storagepath, timeout, **kwargs):
        # Get original command output
        return self._bringonline_cmd('-t', timeout, storagepath, **kwargs)

    def _replicate(self, source_storagepath, destination_storagepath, remotepath, **kwargs):
        chain = CommandChain()
        it = kwargs.pop('_iter', False)
        kwargs['_iter'] = True
        chain.add(self._cp_cmd, '-v', '-p', '--checksum', 'ADLER32', source_storagepath, destination_storagepath, **kwargs)
        chain.add(self._register_cmd, '-v', 'lfn:'+remotepath, destination_storagepath, **kwargs)
        return self._iterable_output_from_iterable(chain(), _iter=it)

    def _get(self, storagepath, localpath, **kwargs):
        # Get original command output
        return self._cp_cmd('-v', '--checksum', 'ADLER32', storagepath, localpath, **kwargs)

    def _put(self, localpath, storagepath, remotepath, **kwargs):
        # Get original command output
        return self._cp_cmd('-v', '-p', '--checksum', 'ADLER32', localpath, storagepath, 'lfn:'+remotepath, **kwargs)

    def _remove(self, storagepath, remotepath, last=False, **kwargs):
        # Get original command output
        chain = CommandChain()
        it = kwargs.pop('_iter', False)
        kwargs['_iter'] = True
        # Delete file
        chain.add(self._del_cmd, '-v', storagepath, **kwargs)
        # Unregister in lfc
        chain.add(self._unregister_cmd, '-v', 'lfn:'+remotepath, storagepath, **kwargs)
        if last:
            # Delete lfn
            chain.add(self._del_cmd, '-v', 'lfn:'+remotepath, **kwargs)
        return self._iterable_output_from_iterable(chain(), _iter=it)

def get_backend(config):
    """Return the backend according to the provided configuration."""

    if config.backend == 'lcg':
        return LCGBackend(basedir = config.basedir)
    if config.backend == 'gfal':
        return GFALBackend(basedir = config.basedir)
    else:
        raise config.ConfigError('backend', "Unknown backend!")
