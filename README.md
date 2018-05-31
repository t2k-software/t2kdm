T2K Data Manager - t2kdm
========================

Install
-------

1.  Clone this repository
2.  Install with pip

        $ pip install [--user] -e .

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
