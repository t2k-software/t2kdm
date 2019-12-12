"""Module handling the command line interface (CLI)."""

from six import print_
import cmd
import sh
import shlex
import argparse
import os
import posixpath
import t2kdm as dm
from t2kdm.commands import all_commands

def ls(*args, **kwargs):
    return [x.name for x in dm.iter_ls(*args, cached=True, **kwargs)]

class T2KDmCli(cmd.Cmd):
    """T2K Data Manager Command Line Interface (CLI)

    The CLI makes it possible to comfortably browse the grid files. All commands
    that are exposed as stand-alone scripts are also available in the CLI. This is
    ensured by registering the Commands in the `all_commands` list in the
    `commands` module.
    """

    intro = """Welcome to the T2K Data Manager CLI.
  ____  ___   _  _  ____  __  __       ___  __    ____
 (_  _)(__ \ ( )/ )(  _ \(  \/  )___  / __)(  )  (_  _)
   )(   / _/ |   (  )(_) ))    ((___)( (__  )(__  _)(_
  (__) (____)(_)\_)(____/(_/\/\_)     \___)(____)(____)

Type 'help' or '?' to list commands.
"""
    prompt = '(%s) '%(dm._branding)

    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)

        # Current directories for relative paths
        self.remotedir = posixpath.abspath('/')
        self.localdir = os.path.abspath(os.getcwd())

    def do_pwd(self, arg):
        """usage: pwd

        Print the current remote directory.
        """
        print_(self.remotedir)

    def do_lpwd(self, arg):
        """usage: lpwd

        Print the current local directory.
        """
        print_(self.localdir)

    def get_abs_remote_path(self, arg):
        """Return absolute remote path."""
        if posixpath.isabs(arg):
            return arg
        else:
            return posixpath.normpath(posixpath.join(self.remotedir, arg))

    def get_abs_local_path(self, arg):
        """Return absolute local path."""
        if os.path.isabs(arg):
            return arg
        else:
            return os.path.normpath(os.path.join(self.localdir, arg))

    def do_cd(self, arg):
        """usage: cd remotepath

        Change the current remote directory.
        """
        pwd = self.get_abs_remote_path(arg)
        # Let us see whether the path exists
        try:
            ls(pwd)
        except dm.backends.DoesNotExistException as e:
            print_(e)
        else:
            # And whether it is a directory
            if dm.is_dir(pwd, cached=True):
                self.remotedir = pwd
            else:
                print_("ERROR, not a directory: %s"%(pwd,))

    def do_lcd(self, arg):
        """usage: cd localpath

        Change the current local diretory.
        """
        pwd = self.get_abs_local_path(arg)
        if os.path.isdir(pwd):
            try:
                os.chdir(pwd)
            except OSError as e: # Catch permission errors
                print_(e)
            self.localdir = os.path.abspath(os.getcwd())
        else:
            print_("ERROR, no such local directory: %s"%(pwd,))

    def do_lls(self, arg):
        """usage: lls [-l] localpath

        List contents of local directory.
        """
        try:
            argv = shlex.split(arg)
        except ValueError as e: # Catch errors from bad bash syntax
            print_(e)
            return False

        try:
            print_(sh.ls('-1', *argv, _bg_exc=False, _tty_out=False), end='')
        except sh.ErrorReturnCode as e:
            print_(e.stderr, end='')

    def do_exit(self, arg):
        """Exit the CLI."""
        return True

    def do_quit(self, arg):
        """Exit the CLI."""
        return True

    def emptyline(self):
        print_()

    def completedefault(self, text, line, begidx, endidx):
        """Complete with content of current remote or local dir."""

        candidates = []

        # The built-in argument parsing is not very good.
        # Let's try our own.
        try:
            args = shlex.split(line)
        except ValueError: # Catch badly formatted strings
            args = line.split()

        if len(args) == 1:
            # Just the main command
            # Text should be empty
            search_text = ''
        else:
            search_text = args[-1]

        text_offset = len(search_text) - len(text)

        # Local commands start with 'l'.
        # Special case 'ls'
        if line[0] == 'l' and line[1] != 's':
            # Local path
            # Look further than just current dir
            searchdir, searchfile = os.path.split(search_text)
            abs_searchdir = searchdir
            if not os.path.isabs(abs_searchdir):
                abs_searchdir = os.path.join(self.localdir, abs_searchdir)
            # Get contents of dir
            for l in sh.ls(abs_searchdir, '-1', _iter=True, _tty_out=False):
                l = l.strip()
                if l.startswith(searchfile):
                    cand = os.path.join(searchdir, l)
                    if os.path.isdir(posixpath.join(abs_searchdir, l)):
                        cand += os.path.sep
                    candidates.append(cand[text_offset:])
        else:
            # Remote path
            # Look further than just current dir
            searchdir, searchfile = posixpath.split(search_text)
            abs_searchdir = searchdir
            if not posixpath.isabs(abs_searchdir):
                abs_searchdir = posixpath.join(self.remotedir, abs_searchdir)
            # Get contents of dir
            for l in ls(abs_searchdir, _iter=True):
                l = l.strip()
                if l.startswith(searchfile):
                    cand = posixpath.join(searchdir, l)
                    if dm.is_dir(posixpath.join(abs_searchdir, l), cached=True):
                        cand += posixpath.sep
                    candidates.append(cand[text_offset:])

        return candidates

# Load all commands into the CLI
# Each `do_X` method in the class is interpreted as a possible command for the CLI.
# Each `help_X` method in the class is called when `help X` is executed.
for command in all_commands:
    do_name = 'do_'+command.name
    # Since this is a method, the first argument will be the CLI instance
    # Also need to pass the command as default value of argument,
    # so it does not change when the variable `command` changes.
    do_cmd = lambda cli, arg, com=command: com.run_from_cli(arg, localdir=cli.localdir, remotedir=cli.remotedir)
    setattr(T2KDmCli, do_name, do_cmd) # Set the `do_X` attribute of the class

    help_name = 'help_'+command.name
    # Since this is a method, the first argument will be the CLI instance
    # Also need to pass the command as default value of argument,
    # so it does not change when the variable `command` changes.
    help_cmd = lambda cli, com=command: com.run_from_cli('-h')
    setattr(T2KDmCli, help_name, help_cmd) # Set the `help_X` attribute of the class

def run_cli():
    """ Start the T2K Data Manager - Command Line Interface."""

    parser = argparse.ArgumentParser(description="Starts the T2K Data Manager - Command Line Interface.")
    args = parser.parse_args()

    try:
        T2KDmCli().cmdloop()
    except KeyboardInterrupt: # Exit gracefully on CTRL-C
        print_('')

if __name__ == '__main__':
    run_cli()
