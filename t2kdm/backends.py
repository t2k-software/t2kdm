"""Backends to be used by the data manager.

Backends do the actual work on the grid and provide the API to do stuff more conveniently.
User output for itneractive commands is handled in the 'interactive' module.

Files can be identified by a couple of different paths/urls.
To avoid confusion, the following convention is used for function arguments:

remotepath
    The logical path of a grid file, as presented to the user.
    Sarts with a '/'.
    Does *not* include the "basedir" of the configuration.

lurl:
    The logical url of the file, as used by the file catalogue.
    It is catalogue specific: catalogue_prefix + basedir + remotepath.

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
import uuid
import time
import re
from hkdm import storage
from hkdm.cache import Cache
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

        basedir: String. Default: '/hyperk.org'
            Sets the base directory of the backend.
            All paths are specified relative to that position.
        """

        self.baseurl = kwargs.pop('catalogue_prefix', '') + kwargs.pop('basedir', '/hyperk.org')
        if len(kwargs) > 0:
            raise TypeError("Invalid keyword arguments: %s"%(list(kwargs.keys),))

    def get_lurl(self, remotepath):
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

        return list(self.iter_ls(remotepath, **kwargs))

    def iter_ls(self, remotepath, **kwargs):
        """List contents of a remote logical path.

        Returns an iterator of directory entries.

        Supported keyword arguments:

        directory: Bool. Default: False
            List directory entries instead of contents.
        """

        lurl = self.get_lurl(remotepath)
        return self._ls(lurl, **kwargs)

    def _ls_se(self, surl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def ls_se(self, remotepath, se, **kwargs):
        """List physical contents of a remote path on a specific SE.

        Supported keyword arguments:

        directory: Bool. Default: False
            List directory entries instead of contents.
        """

        return list(self.iter_ls_se(remotepath, se, **kwargs))

    def iter_ls_se(self, remotepath, se, **kwargs):
        """List physical contents of a remote path on a specific SE.

        Supported keyword arguments:

        directory: Bool. Default: False
            List directory entries instead of contents.
        """

        lurl = self.get_lurl(remotepath)
        ses = storage.get_SE(se)
        if ses is None:
            raise BackendException("Could not find storage element %s."%(se,))
        surl = ses.get_storage_path(remotepath, direct=True)
        return self._ls_se(surl, **kwargs)

    def _is_dir(self, lurl):
        entry = next(self._ls(lurl, directory=True))
        return entry.mode[0] == 'd'

    @cache.cached
    def is_dir(self, remotepath):
        """Is the remote path a directory?"""
        return self._is_dir(self.get_lurl(remotepath))

    def _is_dir_se(self, surl):
        entry = next(self._ls_se(surl, directory=True))
        return entry.mode[0] == 'd'

    @cache.cached
    def is_dir_se(self, remotepath, se):
        """Is the storage path a directory?"""
        se = storage.get_SE(se)
        return self._is_dir_se(se.get_storage_path(remotepath, direct=True))

    def _is_file(self, lurl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def is_file(self, remotepath, **kwargs):
        """Chcek whether a file catalogue entry exists."""
        return self._is_file(self.get_lurl(remotepath), **kwargs)

    @cache.cached
    def is_file_se(self, remotepath, se, **kwargs):
        """Chcek whether a replica actually exists on a storage element."""
        se = storage.get_SE(se)
        return self._exists(se.get_storage_path(remotepath, direct=True), **kwargs)

    def _exists(self, surl, **kwargs):
        raise NotImplementedError()

    @cache.cached
    def exists(self, surl, **kwargs):
        """Chcek whether a surl actually exists."""
        return self._exists(surl, **kwargs)

    def _register(self, surl, lurl, verbose=False, **kwargs):
        raise NotImplementedError()

    def register(self, surl, remotepath, verbose=False, **kwargs):
        """Register a given surl on the file catalogue."""
        lurl = self.get_lurl(remotepath)
        return self._register(surl, lurl, verbose=verbose, **kwargs)

    def _deregister(self, surl, lurl, verbose=False, **kwargs):
        raise NotImplementedError()

    def deregister(self, surl, remotepath, verbose=False, **kwargs):
        """Deregister a given surl from the file catalogue."""
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

    @cache.cached
    def is_online(self, surl):
        """Return `True` if the replica is online."""
        try:
            state = self.state(surl, cached=False)
        except sh.ErrorReturnCode as e:
            # Raise backend failures
            if len(e.stderr) == 0:
                raise BackendException(e.stdout)
            else:
                raise BackendException(e.stderr)

        return state.startswith('ONLINE')

    def _bringonline(self, surl, timeout, verbose=False, **kwargs):
        raise NotImplementedError()

    def bringonline(self, surl, timeout=60*60*6, verbose=False, **kwargs):
        """Try to bring `surl` online within `timeout` seconds.

        Returns `True` when file is online, `False` if not.
        """
        if self.is_online(surl):
            return True
        else:
            return self._bringonline(surl, timeout, verbose=verbose, **kwargs)

    def get_file_source(self, remotepath, source=None, destination=None, tape=False):
        """Return the closest replica and corresponding SE of the given file."""
        return next(self.iter_file_sources(remotepath, source=source, destination=destination, tape=tape))

    def iter_file_sources(self, remotepath, source=None, destination=None, tape=False):
        """Iterate over the closest replicas and corresponding SEs of the given file."""

        # Get source SE
        if source is None:
            if destination is None:
                srclst = storage.get_closest_SEs(remotepath, tape=tape)
                if len(srclst) == 0:
                    raise BackendException("Could not find valid storage element with replica of %s."%(remotepath,))
                for src in srclst:
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
        destination_path = dst.get_storage_path(remotepath)

        if dst.has_replica(remotepath, check_dark=True):
            # Replica already at destination, nothing to do here
            if verbose:
                print_("Replica of %s already present at destination storage element %s."%(remotepath, dst.name,))
            try:
                dark = not dst.has_replica(remotepath)
            except DoesNotExistException:
                dark = True
            if dark:
                if verbose:
                    print_("Replica seems to be dark. Attempting registration.")
                return self.register(destination_path, remotepath, verbose=verbose)
            else:
                # Replica already present, nothing to do.
                return True

        if dst.has_replica(remotepath, check_dark=False):
            raise BackendException("Replica of %s not present at destination storage element %s, but catalogue claims it is. Aborting."%(remotepath, dst.name,))

        failure = None
        for source_path, src in self.iter_file_sources(remotepath, source, destination, tape):
            if verbose:
                print_("Copying %s to %s"%(source_path, destination_path))

            if src.type == 'tape':
                if verbose:
                    print_("Bringing online %s"%(source_path,))
                try:
                    ret = self.bringonline(source_path, timeout=bringonline_timeout, verbose=verbose)
                except BackendException as e:
                    failure = e
                    ret = False
                if ret == False:
                    if verbose:
                        print_("Failed to bring replica online.")
                    continue

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

        # If the remotepath is a directory, append the filename to it
        if remotepath[-1]==posixpath.sep or self.is_dir(remotepath):
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

        if not final and not dst.has_replica(remotepath):
            # Replica already not present at destination, nothing to do here
            if verbose:
                print_("%s\nReplica not present at destination storage element %s."%(remotepath, dst.name,))
            return True

        # Check how many replicas there are
        # If it is only one, refuse to delete it
        replicas = self.replicas(remotepath)
        nrep = 0
        for rep in replicas:
            # Only count non-blacklisted replicas that actually exist
            se = storage.get_SE(rep)
            if se is not None and not se.is_blacklisted() and self.exists(rep):
                nrep += 1

        if not final and nrep <= 1:
            raise BackendException("Only one replica of file left! Aborting.")

        destination_path = dst.get_replica(remotepath)
        if destination_path is None:
            destination_path = dst.get_storage_path(remotepath)
        lurl = self.get_lurl(remotepath)

        if deregister:
            return self.deregister(destination_path, remotepath, verbose=verbose)
        else:
            # Only actually the last one if there is only one replica left
            # And the se is the correct one
            # If there are no replicas at all, also give the "last" flag to remove the empty catalogue entry
            last = (nrep==0) or (nrep==1 and se.name==dst.name)
            return self._remove(destination_path, lurl, last=last, verbose=verbose, **kwargs)

    def _rmdir(self, lurl, verbose=False):
        """Remove the an empty directory from the catalogue."""
        raise NotImplementedError()

    def rmdir(self, remotepath, verbose=False):
        """Remove the an empty directory from the catalogue."""
        if not self.is_dir(remotepath):
            raise DoesNotExistException("No such directory.")

        if len(self.ls(remotepath)) != 0:
            raise BackendException("Directory is not empty!")

        return self._rmdir(self.get_lurl(remotepath), verbose=verbose)

    def _move_replica(self, surl, new_surl, verbose=False):
        """Rename a replica on disk."""
        raise NotImplementedError()

    def move(self, remotepath, new_remotepath, verbose=False):
        """Move a single file to a new position on the grid.

        This reates a new file catalogue entry and moves all replicas to
        correspond to that entry.
        """

        # Append the basename to the new remotepath if it is a directory
        if new_remotepath[-1]==posixpath.sep or self.is_dir(new_remotepath):
            new_remotepath = posixpath.join(new_remotepath, posixpath.basename(remotepath))

        # Make sure destination does not exist already
        if self.is_file(new_remotepath):
            raise BackendException("New file name already exists!")

        # Make sure everything works
        success = True

        # Loop over replicas of the file
        replicas = self.replicas(remotepath)
        if len(replicas) == 0:
            raise BackendException("File has no replicas!")
        for surl in replicas:
            if verbose:
                print_("Moving replica: %s"%(surl))
            # Get target storage elment
            se = storage.get_SE(surl)
            if se is None:
                raise BackendException("Could not find storage element.")
            # Get the new surl
            new_surl = se.get_storage_path(new_remotepath)
            if verbose:
                print_("New surl: %s"%(new_surl))
            # Move the file
            success &= self._move_replica(surl, new_surl, verbose)
            # Register new replica
            if verbose:
                print_("Registering new replica...")
            success &= self.replicate(new_remotepath, se, verbose=verbose)
            # Remove old replica from catalogue
            if verbose:
                print_("Deregistering old surl...")
            success &= self.deregister(surl, remotepath, verbose=verbose)

        # If everything worked out, delete the file
        if success:
            if verbose:
                print_("Removing old catalogue entry...")
            return self.remove(remotepath, se, final=True, verbose=verbose)
        else:
            return False

    def rename(self, remotepath, re_from, re_to, **kwargs):
        """Rename a file using regular expressions."""
        new_remotepath = re.sub(re_from, re_to, remotepath)
        return self.move(remotepath, new_remotepath, **kwargs)

class DIRACBackend(GridBackend):
    """Grid backend using the GFAL command line tools `gfal-*`."""

    def __init__(self, **kwargs):
        GridBackend.__init__(self, catalogue_prefix='', **kwargs)

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
        self._ls_se_cmd = sh.Command('gfal-ls').bake(color='never')
        self._move_cmd = sh.Command('gfal-rename')
        self._mkdir_cmd = sh.Command('gfal-mkdir')

        self._replicate_cmd = sh.Command('dirac-dms-replicate-lfn')
        self._add_cmd = sh.Command('dirac-dms-add-file')

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
        isdir = self.fc.isDirectory(lurl)
        self._check_return_value(isdir)
        return isdir['Value']['Successful'][lurl]

    def _is_file(self, lurl):
        isfile = self.fc.isFile(lurl)
        self._check_return_value(isfile)
        return isfile['Value']['Successful'][lurl]

    def _get_dir_entry(self, lurl, infodict=None):
        """Take a lurl and return a DirEntry."""
        # If no dctionary with the information is specified, get it from the catalogue
        try:
            md = infodict['MetaData']
        except TypeError:
            md = self.fc.getFileMetadata(lurl)
            if not md['OK']:
                raise BackendException("Failed to list path '%s': %s", lurl, md['Message'])
            for path, error in md['Value']['Failed'].items():
                if 'No such file' in error:
                    # File does not exist, maybe a directory?
                    md = self.fc.getDirectoryMetadata(lurl)
                    for path, error in md['Value']['Failed'].items():
                        raise DoesNotExistException("No such file or directory.")
                else:
                    raise BackendException(md['Value']['Failed'][lurl])
            md = md['Value']['Successful'][lurl]
        return DirEntry(posixpath.basename(lurl), mode=oct(md['Mode']), links=md.get('links', -1), gid=md['OwnerGroup'], uid=md['Owner'], size=md.get('Size', -1), modified=str(md['ModificationDate']))

    def _iter_directory(self, lurl):
        """Iterate over entries in a directory."""

        ret = self.fc.listDirectory(lurl)
        if not ret['OK']:
            raise BackendException("Failed to list path '%s': %s", lurl, lst['Message'])
        for path, error in ret['Value']['Failed'].items():
            if 'Directory does not' in error:
                # Dir does not exist, maybe a File?
                if self.fc.isFile(lurl):
                    lst = [(lurl, None)]
                    break
                else:
                    raise DoesNotExistException("No such file or Directory.")
            else:
                raise BackendException(ret['Value']['Failed'][lurl])
        else:
            # Sort items by keys, i.e. paths
            lst = sorted(ret['Value']['Successful'][lurl]['Files'].items() + ret['Value']['Successful'][lurl]['SubDirs'].items())

        for item in lst:
            yield item # = path, dict

    def _ls(self, lurl, **kwargs):
        # Translate keyword arguments
        d = kwargs.pop('directory', False)

        if d:
            # Just the requested entry itself
            yield self._get_dir_entry(lurl)
            return

        for path, info in self._iter_directory(lurl):
            yield self._get_dir_entry(path, info)

    def _ls_se(self, surl, **kwargs):
        # Translate keyword arguments
        d = kwargs.pop('directory', False)
        args = []
        if -d:
            args.append('-d')
        args.append('-l')
        args.append(surl)
        try:
            output = self._ls_se_cmd(*args, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or Directory.")
            else:
                raise BackendException(e.stderr)
        for line in output:
            fields = line.split()
            mode, links, gid, uid, size = fields[:5]
            name = fields[-1]
            modified = ' '.join(fields[5:-1])
            yield DirEntry(name, mode=mode, links=int(links), gid=gid, uid=uid, size=int(size), modified=modified)

    def _replicas(self, lurl, **kwargs):
        # Check the lurl actually exists
        self._ls(lurl, directory=True)

        rep = self.fc.getReplicas(lurl)
        self._check_return_value(rep)
        rep = rep['Value']['Successful'][lurl]

        return rep.values()

    def _exists(self, surl, **kwargs):
        try:
            ret = self._ls_se_cmd(surl, '-d', '-l', **kwargs).strip()
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                return False
            else:
                if len(e.stderr) == 0:
                    raise BackendException(e.stdout)
                else:
                    raise BackendException(e.stderr)
        else:
            return ret[0] != 'd' # Return `False` for directories

    def _register(self, surl, lurl, verbose=False, **kwargs):
        # Register an existing physical copy in the file catalogue
        se = storage.get_SE(surl).name
        # See if file already exists in DFC
        ret = self.fc.getFileMetadata(lurl)
        try:
            self._check_return_value(ret)
        except DoesNotExistException:
            # Add new file
            size = next(self._ls_se(surl, directory=True)).size
            checksum = self.checksum(surl)
            guid = str(uuid.uuid4()) # The guid does not seem to be important. Make it unique if possible.
            ret = self.dm.registerFile((lurl, surl, size, se, guid, checksum))
        else:
            # Add new replica
            ret = self.dm.registerReplica((lurl, surl, se))

        self._check_return_value(ret)
        if verbose:
            print_("Successfully registered replica %s of %s from %s."%(surl, lurl, se))
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
        # Just send a single short request, then check regularly

        if verbose:
            out = sys.stdout
        else:
            out = None

        end = time.time() + timeout

        try:
            self._bringonline_cmd('-t', 10, surl, _out=out, **kwargs)
        except sh.ErrorReturnCode:
            # The command fails if the file is not online
            # To be expected after 10 seconds
            pass

        wait = 5
        while(True):
            if verbose:
                print_("Checking replica state...")
            if self.is_online(surl):
                if verbose:
                    print_("Replica brought online.")
                return True

            time_left = end - time.time()
            if time_left <= 0:
                if verbose:
                    print_("Could not bring replica online.")
                return False

            wait *= 2
            if time_left < wait:
                wait = time_left

            if verbose:
                print_("Timeout remaining: %d s"%(time_left))
                print_("Checking again in: %d s"%(wait))
            time.sleep(wait)

    def _replicate(self, source_surl, destination_surl, lurl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None

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
        se = storage.get_SE(surl).name

        if last:
            # Delete lfn
            if verbose:
                print_("Removing all replicas of %s."%(lurl,))
            ret = self.dm.removeFile([lurl])
        else:
            if verbose:
                print_("Removing replica of %s from %s."%(lurl, se))
            ret = self.dm.removeReplica(se, [lurl])

        if not ret['OK']:
            raise BackendException('Failed: %s'%(ret['Message']))

        for lurl, error in ret['Value']['Failed'].items():
            if 'No such file' in error:
                raise DoesNotExistException("No such file or directory.")
            else:
                raise BackendException(error)

        return True

    def _rmdir(self, lurl, verbose=False):
        """Remove the an empty directory from the catalogue."""
        rep = self.fc.removeDirectory(lurl)
        self._check_return_value(rep)
        return True

    def _move_replica(self, surl, new_surl, verbose=False, **kwargs):
        if verbose:
            out = sys.stdout
        else:
            out = None

        try:
            folder = posixpath.dirname(new_surl)
            self._mkdir_cmd(folder, '-p', _out=out, **kwargs)
            self._move_cmd(surl, new_surl, _out=out, **kwargs)
        except sh.ErrorReturnCode as e:
            if 'No such file' in e.stderr:
                raise DoesNotExistException("No such file or directory.")
            else:
                if len(e.stderr) == 0:
                    raise BackendException(e.stdout)
                else:
                    raise BackendException(e.stderr)
        return True

def get_backend(config):
    """Return the backend according to the provided configuration."""

    if config.backend == 'lcg':
        from hkdm.legacy_backends import LCGBackend
        return LCGBackend(basedir = config.basedir)
    if config.backend == 'gfal':
        from hkdm.legacy_backends import GFALBackend
        return GFALBackend(basedir = config.basedir)
    if config.backend == 'dirac':
        return DIRACBackend(basedir = config.basedir)
    else:
        raise config.ConfigError('backend', "Unknown backend!")
