T2K Data Manager - t2kdm
========================

Install
-------

1.  Clone this repository
2.  Install with pip

        $ pip install [--user] -e .

Configuration
-------------

Simply run:

    $ t2kdm-config

Test
----

Make sure you have a valid grid proxy.
Then simply run:

    $ t2kdm-tests

Scripts
-------

The T2K Data Manager provides a bunch of scripts for handling the data.
They all start with `t2kdm-*` and come with (basic) instructions when called with `-h`.

Command Line Interface
----------------------

The T2K Data Manager also provides a command line interface (CLI),
which behaves similar to the classic `ftp` client:

    $ t2kdm-cli
    Welcome to the T2K Data Manager CLI.
      ____  ___   _  _  ____  __  __       ___  __    ____
     (_  _)(__ \ ( )/ )(  _ \(  \/  )___  / __)(  )  (_  _)
       )(   / _/ |   (  )(_) ))    ((___)( (__  )(__  _)(_
      (__) (____)(_)\_)(____/(_/\/\_)     \___)(____)(____)

    Type 'help' or '?' to list commands.

    (t2kdm)

The CLI can be quit by typing `quit`, `exit`, or simply pushing `CTRL-C`.

Examples
--------

List files on the grid:

    $ t2kdm-ls /test/t2kdm

Download a file from the grid:

    $ t2kdm-get /test/t2kdm/test1.txt

Download all files in a folder that match a regular expression:

    $ t2kdm-get /test/t2kdm -r 'test[1-3]\.txt'

List replicas of a file:

    $ t2kdm-replicas /test/t2kdm/test1.txt

List all available storage elements:

    $ t2kdm-SEs

Recursively replicate a folder to a specific storage element:

    $ t2kdm-replicate /test/t2kdm UKI-SOUTHGRID-OX-HEP-disk -r

Check which files are replicated to a given storage element:

    $ t2kdm-check /test/t2kdm -s UKI-SOUTHGRID-OX-HEP-disk -r

Remove replicas of files from a specififc storage element:

    $ t2kdm-remove /test/t2kdm/test1.txt UKI-SOUTHGRID-OX-HEP-disk
