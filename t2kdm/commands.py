"""Module handling the CLI and stand-alone script commands."""

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
        """
        args = self.parser.parse_args()
        return self.run(args, **kwargs)

    def run_from_cli(self, argstring, **kwargs):
        """Run command from command line interface.

        Parses single string into arguments.
        """
        args = shlex.split(argstring)
        return self.run_from_arglist(args, **kwargs)

    def run_from_arglist(self, arglist, **kwargs):
        """Run command with list of arguments."""
        args = self.parser.parse_args(arglist)
        return self.run(args, **kwargs)

    @staticmethod
    def _condition_argument(name, value, kwargs):
        """Apply some processing to the aetguments when needed."""

        # Make local paths absolute
        localdir = kwargs.pop('localdir', None)
        if localdir is not None and 'localpath' in name and not path.isabs(value):
            value = path.normpath(path.join(localdir, value))

        # Make remote paths absolute
        remotedir = kwargs.pop('remotedir', None)
        if remotedir is not None and 'remotepath' in name and not posixpath.isabs(value):
            value = posixpath.normpath(posixpath.join(remotedir, value))

        return value

    def run(self, parsed_args, **kwargs):
        """Run the command.

        Takes an parsed arguments object from argparse as input.
        """

        pos_args = []
        for arg in self.positional_arguments:
            value = self._condition_argument(arg, getattr(parsed_args, arg), kwargs)
            pos_args.append(value)
        key_args = {}
        for arg in self.keyword_arguments:
            value = self._condition_argument(arg, getattr(parsed_args, arg), kwargs)
            key_args[arg] = value
        key_args.update(kwargs)

        return self(*pos_args, **key_args)

    def __call__(self, *args, **kwargs):
        """Call the underlying function directly."""
        return self.function(*args, **kwargs)

ls = Command('ls', t2kdm.backend.ls, "List contents of a remote logical path.")
ls.add_argument('remotepath', type=str,
    help="the logical path, e.g. '/nd280'")
ls.add_argument('-l', '--long', action='store_true',
    help="longer, more detailed output")
all_commands.append(ls)
