T2K Data Manager - t2kdm
========================

Install
-------

You will need a working setup of the DIRAC UI to access the grid data.
Follow the instructions here:
https://gridpp.ac.uk/wiki/Quick_Guide_to_Dirac#Dirac_client_installation

DIRAC comes with its own Python interpreter. When you source DIRAC's bashrc,
you will havea somewhat isolated Python environment. Within this environment
you can install t2kdm releases directly with pip:

    $ pip install t2kdm

Or you can clone and install the HEAD version from the repository:

    $ git clone git@github.com:t2k-software/t2kdm.git
    $ pip install -e ./t2kdm

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

Contributing
------------

Please use `pre-commit <https://pre-commit.com/>`_ to check your code before
checking in anything. To install pre-commit and the repository hooks just run
these commands inside the repository:

    $ pip3 install pre-commit
    $ pre-commit install

That's it. Now pre-commit should check your code for compliance whenever you
try to check it in.

Among other things, it will check that the code adheres to the `Black
<https://black.readthedocs.io/en/stable/index.html>`_ code style.
