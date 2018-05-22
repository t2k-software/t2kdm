"""Tests for the T2K data Manager."""

from six import print_
import t2kdm
import t2kdm.commands as cmd
import t2kdm.cli
from contextlib import contextmanager
import sys, os

@contextmanager
def no_output():
    stdout = sys.stdout
    stderr = sys.stderr
    with open(os.devnull, 'w') as f:
        try:
            sys.stdout = f
            sys.stderr = f
            yield
        finally:
            sys.stdout = stdout
            sys.stderr = stderr

@contextmanager
def fake_argv(fake_argv):
    true_argv = sys.argv
    try:
        sys.argv = fake_argv
        yield
    finally:
        sys.stdout = true_argv

def run_read_only_tests(backend = t2kdm.backend):
    print_("Testing ls...")
    assert('nd280' in backend.ls('/'))
    assert('nd280' in backend.ls('/', long=True))

    print_("Testing Commands...")
    with no_output():
        cmd.ls.run_from_cli('-l /') # This should work
        cmd.ls.run_from_cli('.') # This should work
        cmd.ls.run_from_cli('abc') # This should not work, but not throw exception
        cmd.ls.run_from_cli('"abc') # This should not work, but not throw exception
        assert(cmd.ls.run_from_console() == 0) # This should work
        with fake_argv(['t2kdm-cli']):
            assert(cmd.ls.run_from_console() == 0) # This should work
        with fake_argv(['t2kdm-ls', '/abcxyz']):
            assert(cmd.ls.run_from_console() == 1) # This should not work, hence the 1 return value

    print_("Testing CLI...")
    cli = t2kdm.cli.T2KDmCli()
    with no_output():
        cli.onecmd('help ls')
        cli.onecmd('ls .')
        cli.onecmd('cd /nd280')
        cli.onecmd('cd ..')
        cli.onecmd('cd /abcxyz')
        cli.onecmd('lcd /')
        cli.onecmd('lcd .')
        cli.onecmd('lcd /root')
        cli.onecmd('lcd /abcxyz')
        cli.onecmd('lls .')
        cli.onecmd('lls ".')
        assert(cli.completedefault('28', 'ls nd28', 0, 0) == ['280'])
        assert(cli.completedefault('s', 'lls us', 0, 0) == ['sr'])
        assert(cli.completedefault('"us', 'lls "us', 0, 0) == [])

def run_read_write_tests(backend = t2kdm.backend):
    pass

def run_tests():
    import argparse

    parser = argparse.ArgumentParser(description="Run tests for the T2K Data Manaer.")
    parser.add_argument('-w', '--write', action='store_true',
        help="do write tests. Default: read only")

    args = parser.parse_args()
    run_read_only_tests()
    if args.write:
        run_read_write_tests()

if __name__ == '__main__':
    run_tests()
