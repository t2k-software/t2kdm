"""Module handling the CLI and stand-alone script commands."""

from six import print_
import sh
import shlex
import argparse
import t2kdm
import sys
from os import path
import posixpath

all_commands = []

class Command(object):
    """Commands to be used as stand alone scripts and within the CLI.

    Command line arguments are translated to the underlying function's
    positional and keyword arguments. Keyword arguments *must* define a short
    and long version.

    Example:

        ls = Command('ls', t2kdm.backend.ls, "List contents of a remote logical path.")
        ls.add_argument('remotepath', type=str,
            help="the logical path, e.g. '/nd280'")
        ls.add_argument('-l', '--long', action='store_true',
            help="longer, more detailed output")

    The arguments containing `localpath` or `remotepath` are treated specially.
    These arguments are converted into an absolute path if the `localdir` or
    `remotedir` is specified repectively.
    """

    def __init__(self, name, function, description=""):
        self.name = name
        self.function = function
        # Set prog to command name, iff we are running in the CLI
        if "t2kdm-cli" in sys.argv[0]:
            self.parser = argparse.ArgumentParser(prog=name, description=description)
        else:
            self.parser = argparse.ArgumentParser(description=description)
        self.positional_arguments = []
        self.keyword_arguments = []

    def add_argument(self, *args, **kwargs):
        """Add an argument to the parser and memorize how to pass parsed object to the original function.

        All positional arguments are identified by their first argument *not* starting with a '-'.
        They are passed to the original function as `*args`.

        All keyword arguments are identified by their first argument starting with a '-'.
        Their keys are taken from the second argument minus the leading '--'.
        They are passed to the original function as `**kwargs`.
        """

        if args[0][0] == '-':
            # Keyword argument
            self.keyword_arguments.append(args[1][2:])
        else:
            # Positionsl argument
            self.positional_arguments.append(args[0])

        # Add the arguments to the parser
        self.parser.add_argument(*args, **kwargs)

    def run_from_console(self, **kwargs):
        """Entry point for console scripts.

        Parses command line arguments from sys.argv.
        Prints output on screen.
        Returns error code (0 if successful).
        """

        kwargs1 = {
            '_iter': True,
            '_bg_exc': False,
        }
        kwargs1.update(kwargs)

        out = kwargs1.pop('_out', sys.stdout)
        try: # We do *not* want to exit after printing a help message or erroring, so we have to catch that.
            for line in self.run(self.parser.parse_args(), **kwargs1):
                print_(line, file=out, end='')
        except sh.ErrorReturnCode as e:
            print_(e.stderr, file=sys.stderr, end='')
            return e.exit_code

        return 0

    def run_from_cli(self, argstring, **kwargs):
        """Run command from command line interface.

        Parses single string into arguments.
        Prints output on screen.
        Returns `False`.
        """

        try:
            args = shlex.split(argstring)
        except ValueError as e: # Catch errors from bad bash syntax
            print_(e)
            return False

        kwargs1 = {
            '_iter': True,
            '_err_to_out': True,
            '_bg_exc': False,
        }
        kwargs1.update(kwargs)

        out = kwargs1.pop('_out', sys.stdout)
        try: # We do *not* want to exit after printing a help message or erroring, so we have to catch that.
            for line in self.run_from_arglist(args, **kwargs1):
                print_(line, file=out, end='')
        except sh.ErrorReturnCode as e:
            print_(e.stderr, file=out, end='')
            return False
        except SystemExit:
            pass

        return False

    def run_from_arglist(self, arglist, **kwargs):
        """Run command with list of arguments."""
        args = self.parser.parse_args(arglist)
        return self.run(args, **kwargs)

    @staticmethod
    def _condition_argument(name, value, localdir=None, remotedir=None):
        """Apply some processing to the aetguments when needed."""

        # Make local paths absolute
        if localdir is not None and 'localpath' in name and not path.isabs(value):
            value = path.normpath(path.join(localdir, value))

        # Make remote paths absolute
        if remotedir is not None and 'remotepath' in name and not posixpath.isabs(value):
            value = posixpath.normpath(posixpath.join(remotedir, value))

        return value

    def run(self, parsed_args, **kwargs):
        """Run the command.

        Takes an parsed arguments object from argparse as input.
        """

        localdir = kwargs.pop('localdir', None)
        remotedir = kwargs.pop('remotedir', None)

        pos_args = []
        for arg in self.positional_arguments:
            value = self._condition_argument(arg, getattr(parsed_args, arg), localdir=localdir, remotedir=remotedir)
            pos_args.append(value)

        key_args = {}
        for arg in self.keyword_arguments:
            value = self._condition_argument(arg, getattr(parsed_args, arg), localdir=localdir, remotedir=remotedir)
            key_args[arg] = value
        key_args.update(kwargs)

        return self(*pos_args, **key_args)

    def __call__(self, *args, **kwargs):
        """Call the underlying function directly."""
        return self.function(*args, **kwargs)

ls = Command('ls', t2kdm.ls, "List contents of a remote logical path.")
ls.add_argument('remotepath', type=str, nargs='?', default='',
    help="the remote logical path, e.g. '/nd280'")
ls.add_argument('-l', '--long', action='store_true',
    help="longer, more detailed output")
all_commands.append(ls)

replicas = Command('replicas', t2kdm.replicas, "List replicas of a remote logical path.")
replicas.add_argument('remotepath', type=str, nargs='?', default='',
    help="the remote logical path, e.g. '/nd280/file.txt'")
replicas.add_argument('-l', '--long', action='store_true',
    help="longer, more detailed output")
all_commands.append(replicas)

replicate = Command('replicate', t2kdm.replicate, "Replicate file to a storage element.")
replicate.add_argument('remotepath', type=str,
    help="the remote logical path, e.g. '/nd280/file.txt'")
replicate.add_argument('destination', type=str,
    help="the destination storage element by name, e.g. 'UKI-SOUTHGRID-RALPP-disk', or by host, e.g. 't2ksrm.nd280.org'")
replicate.add_argument('-s', '--source', type=str, default=None,
    help="the source storage element by name, e.g. 'UKI-SOUTHGRID-RALPP-disk', or by host, e.g. 't2ksrm.nd280.org'. If no source is provided, the replica closest to the destination is chosen")
replicate.add_argument('-t', '--tape', action='store_true',
    help="accept tape storage elements when choosing the closest one")
all_commands.append(replicate)

get = Command('get', t2kdm.get, "Download file from grid.")
get.add_argument('remotepath', type=str,
    help="the remote logical path, e.g. '/nd280/file.txt'")
get.add_argument('localpath', type=str, nargs='?', default='./',
    help="the local path")
get.add_argument('-s', '--source', type=str, default=None,
    help="the source storage element by name, e.g. 'UKI-SOUTHGRID-RALPP-disk', or by host, e.g. 't2ksrm.nd280.org'. If no source is provided, the replica closest to the destination is chosen")
get.add_argument('-t', '--tape', action='store_true',
    help="accept tape storage elements when choosing the closest one")
all_commands.append(get)

SEs = Command('SEs', t2kdm.list_storage_elements, "Print all available storage elements on screen.")
all_commands.append(SEs)
