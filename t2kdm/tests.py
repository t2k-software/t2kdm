"""Tests for the T2K data Manager."""

import t2kdm as dm
import t2kdm.commands as cmd
import t2kdm.cli as cli
from  t2kdm import backends
from  t2kdm import storage
from  t2kdm import utils

import argparse
from six import print_
from contextlib import contextmanager
import sys, os, sh
import tempfile
import posixpath
import re

testdir = '/test/%s'%(dm._branding,)
testfiles = ['test1.txt', 'test2.txt', 'test3.txt']
testpaths = [posixpath.join(testdir, x) for x in testfiles]
testSEs = ['UKI-SOUTHGRID-RALPP-disk', 'CA-SFU-T21-disk', 'RAL-LCG2-T2K-tape']

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

@contextmanager
def temp_dir():
    tempdir = tempfile.mkdtemp()
    try:
        yield tempdir
    finally:
        sh.rm('-r', tempdir, _tty_out=False)

def run_read_only_tests(tape=False, parallel=2):
    print_("Testing ls...")

    entries = dm.backend.ls(testdir)
    for e in entries:
        if e.name == testfiles[0]:
            break
    else:
        raise Exception("Test file not in listing.")

    print_("Testing ls_se...")

    entries = dm.backend.ls_se(testdir, se=testSEs[0])
    for e in entries:
        if e.name == testfiles[0]:
            break
    else:
        raise Exception("Test file not in listing.")

    print_("Testing is_dir...")
    assert(dm.is_dir(testdir))

    print_("Testing is_dir_se...")
    assert(dm.is_dir_se(testdir, se=testSEs[0]))

    print_("Testing replicas...")
    for rep in dm.backend.replicas(testpaths[0]):
        if 'heplnx204.pp.rl.ac.uk' in rep:
            break
    else:
        raise Exception("Did not find expected replica.")

    print_("Testing iter_file_sources...")
    for rep, se in dm.iter_file_sources(testpaths[0]):
        if 'heplnx204.pp.rl.ac.uk' in rep:
            break
    else:
        raise Exception("Did not find expected replica.")

    print_("Testing is_file...")
    assert(dm.backend.is_file(testpaths[0]))
    assert(not dm.backend.is_file(testpaths[0]+"DNE"))

    print_("Testing is_file_se...")
    assert(dm.backend.is_file_se(testpaths[0], testSEs[0]))
    assert(not dm.backend.is_file_se(testpaths[0]+"DNE", testSEs[0]))

    print_("Testing exists...")
    assert(dm.backend.exists(rep))
    assert(not dm.backend.exists(posixpath.dirname(rep)))

    print_("Testing checksum...")
    assert(dm.backend.checksum(rep) == '529506c1')

    print_("Testing state...")
    assert('ONLINE' in dm.backend.state(rep))

    print_("Testing is_online...")
    assert(dm.backend.is_online(rep))

    print_("Testing StorageElement...")
    # Test distance calculation
    assert(storage.SEs[0].get_distance(storage.SEs[1]) < 0)
    # Test getting SE by host
    host = storage.SEs[0].host
    assert(host in storage.SE_by_host[host].get_storage_path('/test'))
    # Test storage path arithmetic
    assert(storage.SEs[0].get_logical_path(storage.SEs[0].get_storage_path('/test')) == '/test')
    # Test getting the closest SE
    assert(storage.get_closest_SE(testpaths[0]) is not None)

    print_("Testing get...")
    with temp_dir() as tempdir:
        path = testpaths[0]
        filename = os.path.join(tempdir, testfiles[0])

        # Test choosing source SE automatically
        assert(dm.backend.get(path, tempdir) == True)
        assert(os.path.isfile(filename))

        # Test tape refusal
        try:
            with no_output():
                dm.backend.get(testpaths[2], tempdir)
        except backends.BackendException as e:
            assert("Could not find" in e.args[0])
        else:
            raise Exception("Should have refused to download from tape!")

        # Test providing the source SE (RAL tape!)
        if tape:
            print_("From tape!")
            source = testSEs[2]
        else:
            source = testSEs[0]
        try:
            dm.backend.get(path, tempdir, source=source, force=False)
        except backends.BackendException as e:
            assert("already exist" in e.args[0])
        else:
            raise Exception("Should have refused to overwrite!")
        assert(dm.backend.get(path, tempdir, source=source, force=True) == True)
        assert(os.path.isfile(filename))
        os.remove(filename)

        # Test recursive get
        assert(dm.interactive.get(testdir, tempdir, recursive='test[12]\.txt', parallel=parallel) == 0)
        assert(os.path.isfile(filename))

    print_("Testing check...")
    with temp_dir() as tempdir:
        filename = os.path.join(tempdir, 'faulty.txt')
        with no_output(True):
            assert(dm.interactive.check(testdir, checksum=True, se=testSEs, recursive=True, quiet=False, verbose=True, list=filename, parallel=parallel) != 0) # There are some deliberate failures here
        assert os.path.isfile(filename)
        assert os.path.getsize(filename) > 0
        with no_output(True):
            assert(dm.interactive.check(testdir, se=testSEs[0:1], recursivese=testSEs[0], quiet=False, verbose=True) == 0)

    print_("Testing HTML index...")
    with temp_dir() as tempdir:
        utils.html_index("/test/", tempdir)
        utils.html_index("/test/", tempdir, recursive=True)

    print_("Testing Commands...")
    with no_output(True):
        assert(cmd.ls.run_from_cli('-l /') == False)
        assert(cmd.ls.run_from_cli('/', _return=True) == 0)

        assert(cmd.ls.run_from_cli('abc') == False) # This should not work, but not throw exception
        assert(cmd.ls.run_from_cli('"abc', _return=True) != 0) # This should not work, but not throw exception
        with fake_argv(['%s-ls'%(dm._branding,), '/']):
            assert(cmd.ls.run_from_console() == 0) # This should work
        with fake_argv(['%s-cli'%(dm._branding), '/']):
            assert(cmd.ls.run_from_console() == 0) # This should work
        with fake_argv(['%s-ls'%(dm._branding), '/abcxyz']):
            assert(cmd.ls.run_from_console() != 0) # This should not work, hence the not 0 return value

        # None of the Commands should return True in the CLI
        for com in cmd.all_commands:
            assert(com.run_from_cli('') == False)

    print_("Testing CLI...")
    cli = dm.cli.T2KDmCli()
    with no_output(True):
        cli.onecmd('help ls')
        cli.onecmd('ls .')
        cli.onecmd('cd /test')
        cli.onecmd('cd ..')
        cli.onecmd('cd /abcxyz')
        cli.onecmd('lcd /')
        cli.onecmd('lcd .')
        cli.onecmd('lcd /root')
        cli.onecmd('lcd /abcxyz')
        cli.onecmd('lls .')
        cli.onecmd('lls ".')
        cli.onecmd('lcd /')
        assert(cli.completedefault('28', 'ls nd28', 0, 0) == ['280/'])
        assert(cli.completedefault('s', 'lls us', 0, 0) == ['sr/'])
        assert(cli.completedefault('"us', 'lls "us', 0, 0) == [])

def run_read_write_tests(tape=False, parallel=2):
    print_("Testing replicate...")
    with no_output():
        assert(dm.interactive.replicate(testdir, testSEs[1], recursive=r'^test[1]\.t.t$', verbose=True) == 0)
        assert(dm.interactive.replicate(testdir, testSEs[1], recursive=r'^test[2]\.t.t$', source=testSEs[0], verbose=True) == 0)

    print_("Testing put...")
    with temp_dir() as tempdir:
        tempf = 'thisfileshouldnotbehereforlong.txt'
        filename = os.path.join(tempdir, tempf)
        remotename = posixpath.join(testdir, tempf)
        # Make sure the file does not exist
        try:
            for SE in storage.SEs:
                dm.remove(remotename, SE.name, final=True)
        except backends.DoesNotExistException:
            pass
        # Prepare something to upload
        with open(filename, 'wt') as f:
            f.write("This is testfile #3.\n")
        assert(dm.put(filename, testdir+'/', destination=testSEs[0]))

    print_("Testing move...")
    assert(dm.move(remotename, remotename+'dir/test.txt'))
    assert(dm.move(remotename+'dir/test.txt', remotename))
    try:
        dm.move(remotename, remotename)
    except backends.BackendException as e:
        pass
    else:
        raise Exception("Moving to existing file names should not be possible.")

    print_("Testing rename...")
    # Make sure the file does not exist
    renamed = re.sub('txt', 'TXT', remotename)
    try:
        for SE in storage.SEs:
            dm.remove(renamed, SE.name, final=True)
    except backends.DoesNotExistException:
        pass
    assert(dm.rename(remotename, 'txt', 'TXT'))
    assert(dm.rename(renamed, 'TXT', 'txt'))

    print_("Testing rmdir...")
    assert(dm.rmdir(remotename+'dir/'))
    try:
        dm.rmdir(remotename+'dir/')
    except backends.DoesNotExistException:
        pass
    else:
        raise Exception("Should have failed to delete a dir that is not there.")

    print_("Testing disk SEs...")
    # Replicate test file to all SEs, to see if they all work
    for SE in storage.SEs:
        if SE.type == 'tape' or SE.is_blacklisted():
            # These SEs do not seem to cooperate
            continue
        print_(SE.name)
        assert(dm.replicate(remotename, SE.name) == True)
        assert(SE.has_replica(remotename) == True)

    print_("Testing remove...")
    with no_output():
        assert(dm.interactive.remove(testdir, testSEs[1], recursive=True) == 0) # Remove everything from SE1
    # Remove uploaded file from previous test
    try:
        # This should fail!
        for SE in storage.SEs:
            dm.remove(remotename, SE.name)
    except backends.BackendException as e:
        assert("Only one" in e.args[0])
    else:
        raise Exception("The last copy should not have been removed!")
    # With the `final` argument it should work
    try:
        for SE in storage.SEs:
            dm.remove(remotename, SE.name, final=True)
            assert(SE.has_replica(remotename) == False)
        for SE in storage.SEs:
            dm.remove(remotename, SE.name, final=True)
    except backends.DoesNotExistException:
        # The command will fail when the file no longer exists
        pass
    else:
        raise Exception("This should have raised a DoesNotExistException at some point.")

def run_tests():
    """Test the functions of the data manager."""

    parser = argparse.ArgumentParser(description="Run tests for the T2K Data Manager.")
    parser.add_argument('-w', '--write', action='store_true',
        help="do write tests. Default: read only")
    parser.add_argument('-t', '--tape', action='store_true',
        help="do write tape storage tests. Default: disks only")
    parser.add_argument('-p', '--parallel', default=2, type=int,
        help="specify how many parallel processes to test. Defaul: 2")
    parser.add_argument('-b', '--backend', default=None,
        help="specify which backend to use")

    args = parser.parse_args()
    if args.backend is not None:
        dm.config.backend = args.backend
        dm.backend = backends.get_backend(dm.config)

    run_read_only_tests(tape=args.tape, parallel=args.parallel)
    if args.write:
        run_read_write_tests(tape=args.tape, parallel=args.parallel)

    print_("All done.")

if __name__ == '__main__':
    run_tests()
