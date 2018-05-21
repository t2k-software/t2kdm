import sh

class GridBackend(object):
    """Class that handles the actual work on the grid.

    This is just a base class that other classes must inherit from.
    """

    def __init__(self, **kwargs):
        """Initialise backend.

        Accepts the follwoing keyword arguments:

        basedir: String. Default: ''
            Sets the base directory of the backend.
            All paths are specified relative to that position.
        """

        self.basedir = kwargs.pop('basedir', '')
        if len(kwargs) > 0:
            raise TypeError("Invalid keyword arguments: %s"%(list(kwargs.keys),))

    def _ls(self, remotepath, **kwargs):
        raise NotImplementedError()

    def ls(self, remotepath, **kwargs):
        """List contents of a remote logical path.

        Supported keyword arguments:

        long: Bool. Default: False
            Print a longer, more detailed listing.
        """
        _path = self.basedir+remotepath
        return self._ls(_path, **kwargs)

class LCGBackend(GridBackend):
    """Grid backend using the LCG command line tools `lfc-*` and `lcg-*`."""

    def __init__(self, **kwargs):
        # LFC paths alway put a '/grid' as highest level directory.
        # Let us not expose that to the user.
        kwargs['basedir'] = '/grid'+kwargs.pop('basedir', '')
        GridBackend.__init__(self, **kwargs)

        self._ls_cmd = sh.Command('lfc-ls')

    def _ls(self, remotepath, **kwargs):
        # Translate keyword arguments
        l = kwargs.pop('long', False)
        if l:
            args = ['-l', remotepath]
        else:
            args = [remotepath]
        return self._ls_cmd(*args, **kwargs)
