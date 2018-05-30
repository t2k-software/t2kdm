import sh
import posixpath
from t2kdm import storage

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

        self.basedir = kwargs.pop('basedir', '/t2k.org')
        if len(kwargs) > 0:
            raise TypeError("Invalid keyword arguments: %s"%(list(kwargs.keys),))

    def full_path(self, path):
        """Prepend the base dir to a path."""
        return posixpath.normpath(self.basedir + path)

    def _ls(self, remotepath, **kwargs):
        raise NotImplementedError()

    def ls(self, remotepath, **kwargs):
        """List contents of a remote logical path.

        Supported keyword arguments:

        long: Bool. Default: False
            Print a longer, more detailed listing.
        """
        _path = self.full_path(remotepath)
        return self._ls(_path, **kwargs)

    def _replica_state(self, storagepath, **kwargs):
        """Internal method to get the state of a replica, e.g. 'ONLINE'."""
        raise NotImplementedError()

    def _replicas(self, remotepath, **kwargs):
        raise NotImplementedError()

    def _add_replica_info(self, rep):
        SE = storage.get_SE_by_path(rep)
        if SE is not None:
            return "%-24s %-7s %-7s %s"%(SE.name, SE.type, self._replica_state(rep), rep)
        else:
            return "%-24s %-7s %-7s %s"%('UNKNOWN', 'UNKNOWN', self._replica_state(rep), rep)

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

    def _replicate(self, source_storagepath, destination_storagepath, **kwargs):
        raise NotImplementedError()

    def replicate(self, remotepath, destination, source=None, tape=False, **kwargs):
        """Replicate the file to the specified storage element.

        If no source storage elment is provided, the closest replica is chosen.
        If `tape` is True, tape SEs are considered when choosing the closest one.
        """

        dst = storage.get_SE(destination)
        if source is None:
            src = dst.get_closest_SE(remotepath, tape=tape)
        else:
            src = storage.get_SE(source)

        if dst.has_replica(remotepath):
            # Replica already at destination, nothing to do here
            return self._iterable_output_from_text(
                    "Replica already present at destination storage element %s.\n"%(dst.name,), **kwargs)

        if not src.has_replica(remotepath):
            # Replica not present at source, throw error
            raise sh.ErrorReturnCode_1(
                    "No replica present at source storage element %s"%(src.name,))

        source_path = src.get_replica(remotepath)
        destination_path = dst.get_storage_path(remotepath)
        return self._replicate(source_path, destination_path, **kwargs)

class LCGBackend(GridBackend):
    """Grid backend using the LCG command line tools `lfc-*` and `lcg-*`."""

    def __init__(self, **kwargs):
        # LFC paths alway put a '/grid' as highest level directory.
        # Let us not expose that to the user.
        kwargs['basedir'] = '/grid'+kwargs.pop('basedir', '/t2k.org')
        GridBackend.__init__(self, **kwargs)

        self._proxy_init_cmd = sh.Command('voms-proxy-init')
        self._ls_cmd = sh.Command('lfc-ls')
        self._replicas_cmd = sh.Command('lcg-lr')
        self._replica_state_cmd = sh.Command('lcg-ls')
        self._replicate_cmd = sh.Command('lcg-rep')

    def _ls(self, remotepath, **kwargs):
        # Translate keyword arguments
        l = kwargs.pop('long', False)
        if l:
            args = ['-l', remotepath]
        else:
            args = [remotepath]
        return self._ls_cmd(*args, **kwargs)

    def _replicas(self, remotepath, **kwargs):
        return(self._replicas_cmd('lfn:'+remotepath, **kwargs))

    def _replica_state(self, storagepath, **kwargs):
        path = storagepath.strip()
        it = kwargs.pop('_iter', None)
        try:
            listing = self._replica_state_cmd('-l', path, **kwargs)
        except sh.ErrorReturnCode:
            listing = '- - - - - UNKNOWN'
        return listing.split()[5]

    def _replicate(self, source_storagepath, destination_storagepath, **kwargs):
        kwargs['_err_to_out'] = True # Verbose output is on stderr
        return(self._replicate_cmd('-v', '--checksum', '-d', destination_storagepath, source_storagepath, **kwargs))
