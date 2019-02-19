"""Backends to be used by the t2kdm.

Backends do the actual work on the grid and provide the API to do stuff more conveniently.
User output for itneractive commands is handled in the 'interactive' module.

Files can be identified by a couple of different paths/urls.
To avoid confusion, the following convention is used for function arguments:

remotepath
    The logical path of a grid file, as presented to the user.
    Sarts with a '/'.

lurl:
    The logical url of the file, as used by the file catalogue.
    Starts with 'lfn:/'.

surl
    The storage location of a replica.
    Starts with 'srm://' or something similar.

localpath
    The actual path to a file on the local file system.

"""

import sh
import itertools
import posixpath
import os, sys
from t2kdm import storage
from t2kdm.cache import Cache
from six import print_

# Add the option to cache the output of functions for 60 seconds.
# This is enabled by providing the `cached=True` argument.
cache = Cache(60)

class BackendException(Exception):
    """Exception that is thrown if something goes (horribly) wrong."""
    pass

class DoesNotExistException(BackendException):
    """Thrown when a file/directory does not exist."""
    pass

class DirEntry(object):
    """Class representing a directory entry."""

    def __init__(self, name, mode='?', links=-1, uid=-1, gid=-1, size=-1, modified='?'):
        self.name = name
        self.mode = mode
        self.links = links
        self.uid = uid
        self.gid = gid
        self.size = size
        self.modified = modified

class GridBackend(object):
    """Class that handles the actual work on the grid.

    This is just a base class that other classes must inherit from.
    """

    def __init__(self, **kwargs):
        """Initialise backend.

        Accepts the follwoing keyword arguments:

        basedir: String. Default: '/t2k.org'
            Sets the base directory of the backend.
            All paths are specified relative to that position.
        """

        # LFC paths alway put a '/grid' as highest level directory.
        # Let us not expose that to the user.
        self.baseurl = 'lfn:/grid' + kwargs.pop('basedir', '/t2k.org')
        if len(kwargs) > 0:
            raise TypeError("Invalid keyword arguments: %s"%(list(kwargs.keys),))

    def get_lurl(self, remotepath):
        """Prepend the base dir to a path."""
        return posixpath.normpath(self.baseurl + remotepath)

    def _ls(self, lurl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def ls(self, remotepath, **kwargs):
        """List contents of a remote logical path.

        Returns a list of directory entries.

        Supported keyword arguments:

        directory: Bool. Default: False
            List directory entries instead of contents.
        """

        lurl = self.get_lurl(remotepath)
        return self._ls(lurl, **kwargs)

    def _is_dir(self, lurl):
        entry = self._ls(lurl, directory=True)[0]
        return entry.mode[0] == 'd'

    @cache.cached
    def is_dir(self, remotepath):
        """Is the remote path a directory?"""
        return self._is_dir(self.get_lurl(remotepath))

    def _exists(self, surl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def exists(self, surl, **kwargs):
        """Chcek whether a surl actually exists."""
        return self._exists(surl, **kwargs)

    def _deregister(self, surl, lurl, verbose=False, **kwargs):
        raise NotImplementedError()

    def deregister(self, surl, remotepath, verbose=False, **kwargs):
        """Unregister a given surl from the file catalogue."""
        lurl = self.get_lurl(remotepath)
        return self._deregister(surl, lurl, verbose=verbose, **kwargs)

    def _state(self, surl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def state(self, surl, **kwargs):
        """Return the state of a replica, e.g. 'ONLINE'."""
        return self._state(surl, **kwargs)

    def _checksum(self, surl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def checksum(self, surl, **kwargs):
        """Return the checksum of a replica."""
        return self._checksum(surl)

    def _replicas(self, lurl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def replicas(self, remotepath, **kwargs):
        """Return a list of replica surls of a remote logical path."""

        lurl = self.get_lurl(remotepath)
        return self._replicas(lurl, **kwargs)

    def _bringonline(self, surl, timeout, verbose=False, **kwargs):
        raise NotImplementedError()

    def bringonline(self, surl, timeout=60*60*6, verbose=False, **kwargs):
        """Try to bring `surl` online within `timeout` seconds.

        Returns `True` when file is online, `False` if not.
        """
        return self._bringonline(surl, timeout, verbose=verbose, **kwargs)

    def get_file_source(self, remotepath, source=None, destination=None, tape=False):
        """Return the closest replica and corresponding SE of the given file."""
        return next(self.iter_file_sources(remotepath, source=source, destination=destination, tape=tape))

    def iter_file_sources(self, remotepath, source=None, destination=None, tape=False):
        """Iterate over the closest replicas and corresponding SEs of the given file."""

        # Get source SE
        if source is None:
            if destination is None:
                src = storage.get_closest_SE(remotepath, tape=tape)
                if src is None:
                    raise BackendException("Could not find valid storage element with replica of %s."%(remotepath,))
                yield src.get_replica(remotepath), src
                return
            else:
                dst = storage.get_SE(destination)
                if dst is None:
                    raise BackendException("Could not find storage element %s."%(destination,))
                srclst = dst.get_closest_SEs(remotepath, tape=tape)
                if len(srclst) == 0:
                    raise BackendException("Could not find valid storage element with replica of %s."%(remotepath,))
                else:
                    for src in srclst:
                        yield src.get_replica(remotepath), src
                    return
        else:
            src = storage.get_SE(source)
            if src is None:
                raise BackendException("Could not find storage element %s."%(source,))

            if not src.has_replica(remotepath):
                # Replica not present at source, throw error
                raise BackendException("%s\nNo replica present at source storage element %s"%(remotepath, src.name,))
            yield src.get_replica(remotepath), src
            return

    def _replicate(self, source_surl, destination_surl, lurl, verbose=False, **kwargs):
        raise NotImplementedError()

    def replicate(self, remotepath, destination, source=None, tape=False, verbose=False, bringonline_timeout=60*60*6, **kwargs):
        """Replicate the file to the specified storage element.

        If no source storage elment is provided, the closest replica is chosen.
        If `tape` is `True`, tape SEs are considered when choosing the closest one.
        If `verbose` is True, status messages will be printed to the screen.

        Returns `True` if the replication was succesful, `False` if not.
        """

        lurl = self.get_lurl(remotepath)

        # Get destination SE and check if file is already present
        dst = storage.get_SE(destination)
        if dst is None:
            raise BackendException("Could not find storage element %s."%(destination,))

        if dst.has_replica(remotepath):
            # Replica already at destination, nothing to do here
            if verbose:
                print_("Replica of %s already present at destination storage element %s."%(remotepath, dst.name,))
            return True

        destination_path = dst.get_storage_path(remotepath)
        failure = None
        for source_path, src in self.iter_file_sources(remotepath, source, destination, tape):
            if verbose:
                print_("Copying %s to %s"%(source_path, destination_path))

            if src.type == 'tape':
                if verbose:
                    print_("Bringing online %s"%(source_path,))
                try:
                    ret = self.bringonline(source_path, timeout=bringonline_timeout, verbose=verbose) and self._replicate(source_path, destination_path, lurl, verbose=verbose)
                except BackendException as e:
                    failure = e
                    ret = False
            else:
                try:
                    ret = self._replicate(source_path, destination_path, lurl, verbose=verbose)
                except BackendException as e:
                    failure = e
                    ret = False
            if ret:
                return True

        if failure is not None:
            raise failure
        else:
            return False

    def _get(self, surl, localpath, verbose=False, **kwargs):
        raise NotImplementedError()

    def get(self, remotepath, localpath, source=None, tape=False, force=False, verbose=False, bringonline_timeout=60*60*6, **kwargs):
        """Download a file from the grid.

        If no source storage elment is provided, the closest replica is chosen.
        If `tape` is True, tape SEs are considered when choosing the closest one.
        If `force` is `True`, local files will be overwritten.
        If `verbose` is True, status messages will be printed to the screen.
        """

        # Append the basename to the localpath if it is a directory
        if os.path.isdir(localpath):
            localpath = os.path.join(localpath, posixpath.basename(remotepath))

        # Do not overwrite files unless explicitly told to
        if os.path.isfile(localpath) and not force:
            raise BackendException("File does already exist: %s."%(localpath,))

        # Get the source replica
        failure = None
        for replica, src in self.iter_file_sources(remotepath, source, tape=tape):
            if verbose:
                print_("Copying %s to %s"%(replica, localpath))

            if src.type == 'tape':
                if verbose:
                    print_("Bringing online %s"%(replica,))
                try:
                    ret = self.bringonline(replica, timeout=bringonline_timeout, verbose=verbose) and self._get(replica, localpath, verbose=verbose, **kwargs)
                except BackendException as e:
                    failure = e
                    ret = False
            else:
                try:
                    ret = self._get(replica, localpath, verbose=verbose, **kwargs)
                except BackendException as e:
                    failure = e
                    ret = False
            if ret:
                return True

        if failure is not None:
            raise failure
        else:
            return False

    def _put(self, localpath, surl, remotepath, verbose=False, **kwargs):
        raise NotImplementedError()

    def put(self, localpath, remotepath, destination=None, tape=False, verbose=False, **kwargs):
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
                raise BackendException("Could not find valid storage element")
        else:
            # Use the provided destination
            SE = storage.get_SE(destination)
            if SE is None:
                raise BackendException("Could not find storage element %s."%(destination,))

        # Get the storage path
        surl = SE.get_storage_path(remotepath)

        # Upload and register the file
        lurl = self.get_lurl(remotepath)
        return self._put(localpath, surl, lurl, verbose=verbose, **kwargs)

    def _remove(self, surl, lurl, last=False, verbose=False, **kwargs):
        """Remove the given replica and deregister it from the remotepath.

        If `last` is `True`, this replica is the last and the
        lfc entry should be removed as well.
        """
        raise NotImplementedError()

    def remove(self, remotepath, destination, final=False, verbose=False, deregister=False, **kwargs):
        """Remove the replica of a file from a storage element.

        This command will refuse to remove the last replica of a file
        unless the `final` argument is `True`!
        If `deregister` is `True`, the replica will be removed from the catalogue,
        but the physicalcopy will not be deleted.
        """

        # Get destination SE and check if file is already not present
        dst = storage.get_SE(destination)
        if dst is None:
            raise BackendException("Could not find storage element %s.\n"%(destination,))

        if not dst.has_replica(remotepath):
            # Replica already not present at destination, nothing to do here
            if verbose:
                print_("%s\nReplica not present at destination storage element %s."%(remotepath, dst.name,))
            return True

        # Check how many replicas there are
        # If it is only one, refuse to delete it
        replicas = self.replicas(remotepath)
        nrep = 0
        for rep in replicas:
            # Only count non-blacklisted replicas
            se = storage.get_SE(rep)
            if se is not None and not se.is_blacklisted():
                nrep += 1

        if not final and nrep <= 1:
            raise BackendException("Only one replica of file left! Aborting.")

        destination_path = dst.get_replica(remotepath)
        lurl = self.get_lurl(remotepath)

        if deregister:
            return self.deregister(destination_path, remotepath)
        else:
            return self._remove(destination_path, lurl, last=(nrep<=1), verbose=verbose, **kwargs)

class LCGBackend(GridBackend):
    """Grid backend using the LCG command line tools `lfc-*` and `lcg-*`."""

    def __init__(self, **kwargs):
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

    def _ls(self, lurl, **kwargs):
        # Translate keyword arguments
        d = kwargs.pop('directory', False)
        args = []
        if -d:
            args.append('-d')
        args.append('-l')
        args.append(lurl[4:])
        try:
            output = self._ls_cmd(*args, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or Directory.")
            else:
                raise
        ret = []
        for line in output:
            fields = line.split()
            mode, links, uid, gid, size = fields[:5]
            name = fields[-1]
            modified = ' '.join(fields[5:-1])
            ret.append(DirEntry(name, mode=mode, links=int(links), gid=gid, uid=uid, size=int(size), modified=modified))
        return ret

    def _replicas(self, lurl, **kwargs):
        ret = []
        try:
            output = self._replicas_cmd(lurl, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or Directory.")
        for line in output:
            line = line.strip()
            if len(line) > 0:
                ret.append(line.strip())
        return ret

    def _state(self, surl, **kwargs):
        it = kwargs.pop('_iter', None)
        try:
            listing = self._replica_state_cmd('-l', surl, **kwargs)
        except sh.ErrorReturnCode:
            listing = '- - - - - ?'
        except sh.SignalException_SIGSEGV:
            listing = '- - - - - ?'
        return listing.split()[5]

    def _checksum(self, surl, **kwargs):
        it = kwargs.pop('_iter', None)
        try:
            listing = self._replica_checksum_cmd(surl, **kwargs)
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

    def _bringonline(self, surl, timeout, **kwargs):
        kwargs['_err_to_out'] = True # Verbose output is on stderr
        kwargs.pop('_err', None) # Cannot specify _err and _err_ro_out at same time

        # Get original command output
        return self._bringonline_cmd('-v', '--bdii-timeout', timeout, '--srm-timeout', timeout, '--sendreceive-timeout', timeout, '--connect-timeout', timeout, surl, **kwargs)

    def _replicate(self, source_surl, destination_surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._replicate_cmd('-v', '--sendreceive-timeout', 14400, '--checksum', '-d', destination_surl, source_surl, _out=out, _err_to_out=True, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        return True

    def _get(self, surl, localpath, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._cp_cmd('-v', '--sendreceive-timeout', 14400, '--checksum', surl, localpath, _out=out, _err_to_out=True, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        return os.path.isfile(localpath)


    def _put(self, localpath, surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._cr_cmd('-v', '--sendreceive-timeout', 14400, '--checksum', '-d', surl, '-l', lurl, localpath, _out=out, _err_to_out=True, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        return True

    def _remove(self, surl, lurl, last=False, verbose=True, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            if deregister:
                raise BackendException("Operation not supported by LCG backend.")
            else:
                self._del_cmd('-v', surl, _out=out, _err_to_out=True, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        return True

class GFALBackend(GridBackend):
    """Grid backend using the GFAL command line tools `gfal-*`."""

    def __init__(self, **kwargs):
        GridBackend.__init__(self, **kwargs)

        #self._proxy_init_cmd = sh.Command('voms-proxy-init')
        self._ls_cmd = sh.Command('gfal-ls').bake(color='never')
        self._replicas_cmd = sh.Command('gfal-xattr')
        self._replica_checksum_cmd = sh.Command('gfal-sum')
        self._bringonline_cmd = sh.Command('gfal-legacy-bringonline')
        self._cp_cmd = sh.Command('gfal-copy')
        self._register_cmd = sh.Command('gfal-legacy-register')
        self._deregister_cmd = sh.Command('gfal-legacy-deregister')
        self._del_cmd = sh.Command('gfal-rm')

    def _ls(self, lurl, **kwargs):
        # Translate keyword arguments
        d = kwargs.pop('directory', False)
        args = []
        if -d:
            args.append('-d')
        args.append('-l')
        args.append(lurl)
        try:
            output = self._ls_cmd(*args, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or Directory.")
            else:
                raise BackendException(e.stderr)
        ret = []
        for line in output:
            fields = line.split()
            mode, links, gid, uid, size = fields[:5]
            name = fields[-1]
            modified = ' '.join(fields[5:-1])
            ret.append(DirEntry(name, mode=mode, links=int(links), gid=gid, uid=uid, size=int(size), modified=modified))
        return ret

    def _replicas(self, lurl, **kwargs):
        ret = []
        try:
            output = self._replicas_cmd(lurl, 'user.replicas', **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or Directory.")
            else:
                raise BackendException(e.stderr)
        for line in output:
            line = line.strip()
            if len(line) > 0:
                ret.append(line.strip())
        return ret

    def _exists(self, surl, **kwargs):
        try:
            state = self._replicas_cmd(surl, 'user.status', **kwargs).strip()
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                return False
            else:
                raise BackendException(e.stderr)
        else:
            return True

    def _deregister(self, surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._deregister_cmd(lurl, surl, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        else:
            return True

    def _state(self, surl, **kwargs):
        try:
            state = self._replicas_cmd(surl, 'user.status', **kwargs).strip()
        except sh.ErrorReturnCode:
            state = '?'
        except sh.SignalException_SIGSEGV:
            state = '?'
        return state

    def _checksum(self, surl, **kwargs):
        try:
            checksum = self._replica_checksum_cmd(surl, 'ADLER32', **kwargs).split()[1]
        except sh.ErrorReturnCode:
            checksum = '?'
        except sh.SignalException_SIGSEGV:
            checksum = '?'
        except IndexError:
            checksum = '?'
        return checksum

    def _bringonline(self, surl, timeout, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        # gfal does not notice when files come online, it seems
        # split task into many requests with short timeouts
        if verbose:
            out = sys.stdout
        else:
            out = None
        time_left = timeout
        while(True):
            if time_left > 10:
                timeout = 10
            else:
                timeout = time_left
            time_left -= 10
            try:
                self._bringonline_cmd('-t', timeout, surl, _out=out, **kwargs)
            except sh.ErrorReturnCode:
                # Not online yet.
                if time_left > 0:
                    continue
                else:
                    return False
            else:
                # File is online.
                return True

    def _replicate(self, source_surl, destination_surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._cp_cmd('-p', '-T', '1800', '--checksum', 'ADLER32', source_surl, destination_surl, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            elif 'File exists' in e.stderr:
                if verbose:
                    print_("Replica already exists. Checking checksum...")
                if self.checksum(destination_surl) == self.checksum(source_surl):
                    if verbose:
                        print_("Checksums match. Registering replica.")
                else:
                    raise BackendException("File with different checksum already present.")
            else:
                raise BackendException(e.stderr)

        try:
            self._register_cmd(lurl, destination_surl, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            raise BackendException(e.stderr)

        return True

    def _get(self, surl, localpath, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._cp_cmd('-f', '--checksum', 'ADLER32', surl, localpath, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        return os.path.isfile(localpath)

    def _put(self, localpath, surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._cp_cmd('-p', '--checksum', 'ADLER32', localpath, surl, lurl, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        return True

    def _remove(self, surl, lurl, last=False, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._del_cmd(surl, _out=out, **kwargs)
            self._deregister_cmd(lurl, surl, _out=out, **kwargs)
            if last:
                # Delete lfn
                self._del_cmd(lurl, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(e.stderr)
        return True

class DIRACBackend(GridBackend):
    """Grid backend using the GFAL command line tools `gfal-*`."""

    def __init__(self, **kwargs):
        GridBackend.__init__(self, **kwargs)

        from DIRAC.Core.Base import Script
        Script.initialize()
        from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
        self.pm = ProxyManagerClient()

        proxy = self.pm.getUserProxiesInfo()
        if not proxy['OK']:
            raise BackendException("Proxy error.")

        from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
        self.fc = FileCatalog()
        from DIRAC.DataManagementSystem.Client.DataManager import DataManager
        self.dm = DataManager()

        self._xattr_cmd = sh.Command('gfal-xattr')
        self._replica_checksum_cmd = sh.Command('gfal-sum')
        self._bringonline_cmd = sh.Command('gfal-legacy-bringonline')
        self._cp_cmd = sh.Command('gfal-copy')

        self._replicate_cmd = sh.Command('dirac-dms-replicate-lfn')
        self._add_cmd = sh.Command('dirac-dms-add-file')

    @staticmethod
    def strip_lurl(lurl):
        """Strip te unnecessary stuff from the beginning of lurls.

        lfn:/grid/t2k.org/... -> /t2k.org/...

        """
        return lurl[9:]

    @staticmethod
    def _check_return_value(ret):
        if not ret['OK']:
            raise BackendException("Failed: %s", ret['Message'])
        for path, error in ret['Value']['Failed'].items():
            if ('No such' in error) or ('Directory does not' in error):
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(error)

    def _is_dir(self, lurl):
        lurl = self.strip_lurl(lurl)
        isdir = self.fc.isDirectory(lurl)
        self._check_return_value(isdir)
        return isdir['Value']['Successful'][lurl]

    def _get_dir_entry(self, lurl):
        """Take a lurl and return a DirEntry."""
        md = self.fc.getFileMetadata(lurl)
        if not md['OK']:
            raise BackendException("Failed to list path '%s': %s", lurl, md['Message'])
        if lurl not in md['Value']['Successful']:
            if 'No such file' in md['Value']['Failed'][lurl]:
                # File does not exist, maybe a directory?
                md = self.fc.getDirectoryMetadata(lurl)
                if lurl not in md['Value']['Successful']:
                    raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(md['Value']['Failed'][lurl])
        md = md['Value']['Successful'][lurl]
        return DirEntry(posixpath.basename(lurl), mode=oct(md['Mode']), links=md.get('links', -1), gid=md['OwnerGroup'], uid=md['Owner'], size=md.get('Size', -1), modified=str(md['ModificationDate']))

    def _iter_directory(self, lurl):
        """Iterate over entries in a directory."""

        lst = self.fc.listDirectory(lurl)
        if not lst['OK']:
            raise BackendException("Failed to list path '%s': %s", lurl, lst['Message'])
        if lurl not in lst['Value']['Successful']:
            if 'Directory does not' in lst['Value']['Failed'][lurl]:
                # Dir does not exist, maybe a File?
                if self.fc.isFile(lurl):
                    lst = [lurl]
                else:
                    raise DoesNotExistException("No such file or Directory.")
            else:
                raise BackendException(lst['Value']['Failed'][lurl])
        else:
            lst = sorted(lst['Value']['Successful'][lurl]['Files'].keys() + lst['Value']['Successful'][lurl]['SubDirs'].keys())

        for path in lst:
            yield path

    def _ls(self, lurl, **kwargs):
        # Translate keyword arguments
        d = kwargs.pop('directory', False)
        lurl = self.strip_lurl(lurl)

        if d:
            # Just the requested entry itself
            return [self._get_dir_entry(lurl)]

        ret = []
        for path in self._iter_directory(lurl):
            # TODO: Possible optimisation, listDirectory already conatins all information, is it cached?
            ret.append(self._get_dir_entry(path))

        return ret

    def _replicas(self, lurl, **kwargs):
        # Check the lurl actually exists
        self._ls(lurl, directory=True)
        lurl = self.strip_lurl(lurl)

        rep = self.fc.getReplicas(lurl)
        self._check_return_value(rep)
        rep = rep['Value']['Successful'][lurl]

        return rep.values()

    def _exists(self, surl, **kwargs):
        try:
            state = self._xattr_cmd(surl, 'user.status', **kwargs).strip()
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                return False
            else:
                if len(e.stderr) == 0:
                    raise BackendException(e.stdout)
                else:
                    raise BackendException(e.stderr)
        else:
            return True

    def _deregister(self, surl, lurl, verbose=False, **kwargs):
        # DIRAC only needs to know the SE name to deregister a replica
        se = storage.get_SE(surl).name
        ret = self.dm.removeReplicaFromCatalog(se, [lurl])
        self._check_return_value(ret)
        if verbose:
            print_("Successfully deregistered replica of %s from %s."%(lurl, se))
        return True

    def _state(self, surl, **kwargs):
        try:
            state = self._xattr_cmd(surl, 'user.status', **kwargs).strip()
        except sh.ErrorReturnCode:
            state = '?'
        except sh.SignalException_SIGSEGV:
            state = '?'
        return state

    def _checksum(self, surl, **kwargs):
        try:
            checksum = self._replica_checksum_cmd(surl, 'ADLER32', **kwargs).split()[1]
        except sh.ErrorReturnCode:
            checksum = '?'
        except sh.SignalException_SIGSEGV:
            checksum = '?'
        except IndexError:
            checksum = '?'
        return checksum

    def _bringonline(self, surl, timeout, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        # gfal does not notice when files come online, it seems
        # split task into many requests with short timeouts
        if verbose:
            out = sys.stdout
        else:
            out = None
        time_left = timeout
        while(True):
            if time_left > 10:
                timeout = 10
            else:
                timeout = time_left
            time_left -= 10
            try:
                self._bringonline_cmd('-t', timeout, surl, _out=out, **kwargs)
            except sh.ErrorReturnCode:
                # Not online yet.
                if time_left > 0:
                    continue
                else:
                    return False
            else:
                # File is online.
                return True

    def _replicate(self, source_surl, destination_surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None

        lurl = self.strip_lurl(lurl)
        source = storage.get_SE(source_surl).name
        destination = storage.get_SE(destination_surl).name
        try:
            self._replicate_cmd(lurl, destination, source, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                if len(e.stderr) == 0:
                    raise BackendException(e.stdout)
                else:
                    raise BackendException(e.stderr)

        return True

    def _get(self, surl, localpath, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        try:
            self._cp_cmd('-f', '--checksum', 'ADLER32', surl, localpath, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                if len(e.stderr) == 0:
                    raise BackendException(e.stdout)
                else:
                    raise BackendException(e.stderr)
        return os.path.isfile(localpath)

    def _put(self, localpath, surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None
        lurl = self.strip_lurl(lurl)
        se = storage.get_SE(surl).name

        try:
            self._add_cmd(lurl, localpath, se, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                if len(e.stderr) == 0:
                    raise BackendException(e.stdout)
                else:
                    raise BackendException(e.stderr)
        return True

    def _remove(self, surl, lurl, last=False, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None

        lurl = self.strip_lurl(lurl)
        se = storage.get_SE(surl).name

        if last:
            # Delete lfn
            ret = self.dm.removeFile([lurl])
        else:
            ret = self.dm.removeReplica(se, [lurl])

        if not ret['OK']:
            raise BackendException('Failed: %s'%(ret['Message']))

        for lurl, error in ret['Value']['Failed'].items():
            if 'No such file' in error:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(error)

        return True

def get_backend(config):
    """Return the backend according to the provided configuration."""

    if config.backend == 'lcg':
        return LCGBackend(basedir = config.basedir)
    if config.backend == 'gfal':
        return GFALBackend(basedir = config.basedir)
    if config.backend == 'dirac':
        return DIRACBackend(basedir = config.basedir)
    else:
        raise config.ConfigError('backend', "Unknown backend!")
