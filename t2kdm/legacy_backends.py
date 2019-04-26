"""Old backends no longer in active development.

These might or might not work.
"""

from t2kdm.backends import *

class LCGBackend(GridBackend):
    """Grid backend using the LCG command line tools `lfc-*` and `lcg-*`."""

    def __init__(self, **kwargs):
        GridBackend.__init__(self, catalogue_prefix='lfn:/grid', **kwargs)

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
        for line in output:
            fields = line.split()
            mode, links, uid, gid, size = fields[:5]
            name = fields[-1]
            modified = ' '.join(fields[5:-1])
            yield DirEntry(name, mode=mode, links=int(links), gid=gid, uid=uid, size=int(size), modified=modified)

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
        GridBackend.__init__(self, catalogue_prefix='lfn:/grid', **kwargs)

        #self._proxy_init_cmd = sh.Command('voms-proxy-init')
        self._ls_cmd = sh.Command('gfal-ls').bake(color='never')
        self._replicas_cmd = sh.Command('gfal-xattr')
        self._replica_checksum_cmd = sh.Command('gfal-sum')
        self._bringonline_cmd = sh.Command('gfal-legacy-bringonline')
        self._cp_cmd = sh.Command('gfal-copy')
        self._register_cmd = sh.Command('gfal-legacy-register')
        self._deregister_cmd = sh.Command('gfal-legacy-unregister')
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
        for line in output:
            fields = line.split()
            mode, links, gid, uid, size = fields[:5]
            name = fields[-1]
            modified = ' '.join(fields[5:-1])
            yield DirEntry(name, mode=mode, links=int(links), gid=gid, uid=uid, size=int(size), modified=modified)

    def _ls_se(self, surl, **kwargs):
        return self._ls(surl, **kwargs)

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

