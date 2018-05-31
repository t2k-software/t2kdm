"""Tests for the T2K data Manager."""

import argparse
from six import print_
import t2kdm
import t2kdm.commands as cmd
import t2kdm.cli
import t2kdm.storage
from contextlib import contextmanager
import sys, os
import tempfile

@contextmanager
def no_output(redirect=True):
    stdout = sys.stdout
    stderr = sys.stderr
    with open('/dev/null', 'w') as null:
        try:
            if redirect:
                sys.stdout = null
                sys.stderr = null
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
        sys.sargv = true_argv

def run_read_only_tests(backend = t2kdm.backend):
    print_("Testing ls...")
    assert('nd280' in backend.ls('/'))
    assert('nd280' in backend.ls('/', long=True))

    print_("Testing replicas...")
    assert('srm-t2k.gridpp.rl.ac.uk' in backend.replicas('/nd280/raw/ND280/ND280/00000000_00000999/nd280_00000001_0000.daq.mid.gz'))
    assert('RAL-LCG22-tape' in backend.replicas('/nd280/raw/ND280/ND280/00000000_00000999/nd280_00000001_0000.daq.mid.gz', long=True))

    print_("Testing StorageElement...")
    assert(t2kdm.storage.SEs[0].get_distance(t2kdm.storage.SEs[1]) < 0)
    assert('srm-t2k.gridpp.rl.ac.uk' in t2kdm.storage.SE_by_host['srm-t2k.gridpp.rl.ac.uk'].get_storage_path('/nd280/test'))
    t2kdm.storage.get_closest_SE('/nd280/raw/ND280/ND280/00000000_00000999/nd280_00000001_0000.daq.mid.gz')

    print_("Testing TriumfStorageElement...")
    assert('t2ksrm.nd280.org/nd280data/' in t2kdm.storage.SE_by_host['t2ksrm.nd280.org'].get_storage_path('/nd280/test'))

    print_("Testing get...")
    tempdir = tempfile.mkdtemp()
    t2kdm.get('/test/test.txt', tempdir)
    filename = os.path.join(tempdir, 'test.txt')
    assert(os.path.isfile(os.path.join(tempdir, 'test.txt')))
    os.remove(filename)
    os.rmdir(tempdir)

    print_("Testing Commands...")
    with open('/dev/null', 'w') as out:
        cmd.ls.run_from_cli('-l /', _out=out) # This should work, but return `False`
        cmd.ls.run_from_cli('.', _out=out) # This should work

    with no_output(True):
        cmd.ls.run_from_cli('abc') # This should not work, but not throw exception
        cmd.ls.run_from_cli('"abc') # This should not work, but not throw exception
        with fake_argv(['t2kdm-ls']):
            assert(cmd.ls.run_from_console() == 0) # This should work
        with fake_argv(['t2kdm-cli']):
            assert(cmd.ls.run_from_console() == 0) # This should work
        with fake_argv(['t2kdm-ls', '/abcxyz']):
            assert(cmd.ls.run_from_console() == 1) # This should not work, hence the 1 return value

        # None of the Commands should return True in the CLI
        for com in cmd.all_commands:
            assert(com.run_from_cli('') == False)

    print_("Testing CLI...")
    cli = t2kdm.cli.T2KDmCli()
    with no_output(True):
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
    """Test the functions of the t2kdm."""

    parser = argparse.ArgumentParser(description="Run tests for the T2K Data Manaer.")
    parser.add_argument('-w', '--write', action='store_true',
        help="do write tests. Default: read only")

    args = parser.parse_args()
    run_read_only_tests()
    if args.write:
        run_read_write_tests()

    print_("All done.")

if __name__ == '__main__':
    run_tests()
