HyperK Data Manager - hkdm
==========================

Install
-------

You will need a working setup of the DIRAC UI to access the grid data.
Follow the instructions here:
https://gridpp.ac.uk/wiki/Quick_Guide_to_Dirac#Dirac_client_installation

You will also need a valid Grid certificate. Instructions on how to obtain one are available at: https://t2k.org/nd280/datacomp/gridandirac/gridcert

DIRAC comes with its own Python interpreter. When you source DIRAC's bashrc,
you will havea somewhat isolated Python environment. Within this environment
you can install t2kdm releases directly with pip:

    $ pip install hkdm

Or you can clone and install the HEAD version from the t2kdm repository:

    $ git clone git@github.com:t2k-software/t2kdm.git
    $ cd t2kdm
    $ git checkout hyperk
    $ pip install -e .

Configuration
-------------

Simply run:

    $ hkdm-config

Test
----

Make sure you have a valid grid proxy.
Then simply run:

    $ hkdm-tests

Scripts
-------

The HyperK Data Manager provides a bunch of scripts for handling the data.
They all start with `hkdm-*` and come with (basic) instructions when called with `-h`.

Command Line Interface
----------------------

The HyperK Data Manager also provides a command line interface (CLI),
which behaves similar to the classic `ftp` client:

    $ hkdm-cli
    Welcome to the HyperK Data Manager CLI.
    A fork of the T2K Data Manager.
      ____  ___   _  _  ____  __  __       ___  __    ____
     (_  _)(__ \ ( )/ )(  _ \(  \/  )___  / __)(  )  (_  _)
       )(   / _/ |   (  )(_) ))    ((___)( (__  )(__  _)(_
      (__) (____)(_)\_)(____/(_/\/\_)     \___)(____)(____)

    Type 'help' or '?' to list commands.

    (hkdm)

The CLI can be quit by typing `quit`, `exit`, or simply pushing `CTRL-C`.

Examples
--------

List files on the grid:

    $ hkdm-ls /test/hkdm

Download a file from the grid:

    $ hkdm-get /test/hkdm/test1.txt

Download all files in a folder that match a regular expression:

    $ hkdm-get /test/hkdm -r 'test[1-3]\.txt'

List replicas of a file:

    $ hkdm-replicas /test/hkdm/test1.txt

List all available storage elements:

    $ hkdm-SEs

Recursively replicate a folder to a specific storage element:

    $ hkdm-replicate /test/hkdm UKI-SOUTHGRID-OX-HEP-disk -r

Check which files are replicated to a given storage element:

    $ hkdm-check /test/hkdm -s UKI-SOUTHGRID-OX-HEP-disk -r

Remove replicas of files from a specific storage element:

    $ hkdm-remove /test/hkdm/test1.txt UKI-LT2-IC-HEP-disk

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
