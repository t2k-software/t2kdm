"""Module handling the command line interface (CLI)."""

from six import print_
import cmd
import sh
import argparse
from os import path
import posixpath
from t2kdm.commands import all_commands

class T2KDmCli(cmd.Cmd):
    intro = """Welcome to the T2K Data Manager CLI.
  ____  ___   _  _  ____  __  __       ___  __    ____
 (_  _)(__ \ ( )/ )(  _ \(  \/  )___  / __)(  )  (_  _)
   )(   / _/ |   (  )(_) ))    ((___)( (__  )(__  _)(_
  (__) (____)(_)\_)(____/(_/\/\_)     \___)(____)(____)

Type 'help' or '?' to list commands.
"""
    prompt = '(t2kdm) '

    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)

        self.remotedir = posixpath.abspath('/')
        self.localdir = path.abspath('./')

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
            return path
        else:
            return posixpath.normpath(posixpath.join(self.remotedir, arg))

    def get_abs_local_path(self, arg):
        """Return absolute local path."""
        if path.isabs(arg):
            return path
        else:
            return path.normpath(path.join(self.localdir, arg))

    def do_cd(self, arg):
        """usage: cd remotepath

        Change the current remote diretory.

        Note: Currently there are no checks done whether the remote directory actually exists.
        """
        self.remotedir = self.get_abs_remote_path(arg)

    def do_lcd(self, arg):
        """usage: cd localpath

        Change the current local diretory.
        """
        pwd = self.get_abs_local_path(arg)
        if path.isdir(pwd):
            self.localdir = pwd
        else:
            print_("ERROR, no such local directory: %s"%(pwd,))

    def do_exit(self, arg):
        """Exit the CLI."""
        return True

# Load all commands into the CLI
# Each `do_X` method in the class is interpreted as a possible command for the CLI.
# Each `help_X` method in the class is called when `help X` is executed.
for command in all_commands:
    do_name = 'do_'+command.name
    def do_cmd(cli, arg): # Since this is a method, the first argument will be the CLI instance
        try: # We do *not* want to exit after printing a help message or erroring, so we have to catch that.
            for line in command.run_from_cli(arg, localdir=cli.localdir, remotedir=cli.remotedir,
                    _iter=True, _err_to_out=True, _ok_code=list(range(256))):
                print_(line, end='')
        except SystemExit:
            pass
    setattr(T2KDmCli, do_name, do_cmd) # Set the `do_X` attribute of the class
    help_name = 'help_'+command.name
    def help_cmd(cli): # Since this is a method, the first argument will be the CLI instance
        try: # We do *not* want to exit after printing a help message, so we have to catch that.
            for line in command.run_from_arglist(['-h'], _iter=True):
                print_(line, end='')
        except SystemExit:
            pass
    setattr(T2KDmCli, help_name, help_cmd) # Set the `help_X` attribute of the class

def run_cli():
    try:
        T2KDmCli().cmdloop()
    except KeyboardInterrupt: # Exit gracefully on CTRL-C
        pass

if __name__ == '__main__':
    run_cli()
