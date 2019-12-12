"""Module to deal with regular replication, checking and general housekeeping tasks."""

import argparse
from six.moves import configparser, html_entities
import base64
from six import print_
import t2kdm as dm
import t2kdm.commands as commands
from contextlib import contextmanager
import sys, os
import sh
from datetime import datetime, timedelta, tzinfo
import posixpath
import tempfile

class UTC(tzinfo):
    """UTC class, because pytz would be overkill"""

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)
utc = UTC()

def pid_running(pid):
    """Return `True` is a process with the given PID is running."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

class Task(object):
    """Class to organise what need to be done when."""

    def __init__(self, **kwargs):
        """Initialise a basic task.

        All tasks support these basic keyword arguments:

            frequency = 'daily' | 'weekly' | 'monthly'
                How often is the task to be done? Default: 'weekly'

            logfile
                If provided, redirect all output of the task to this file.

        """

        # Handle basic keyword arguments
        self.frequency = kwargs.pop('frequency', 'weekly')
        if self.frequency not in ['daily', 'weekly', 'monthly']:
            raise ValueError("Illegal frequency!")
        self.logfile = kwargs.pop('logfile', None)

        self.last_done = None
        self.state = None

    def get_period(self):
        """Get the time period (1/frequency) of the task."""
        if self.frequency  == 'daily':
            return timedelta(1)
        elif self.frequency  == 'weekly':
            return timedelta(7)
        elif self.frequency  == 'monthly':
            return timedelta(30)
        else:
            raise ValueError("Illegal frequency!")

    @contextmanager
    def redirected_output(self, append=False):
        if self.logfile is not None:
            # Redirect output to logfile
            stdout = sys.stdout
            stderr = sys.stderr
            # Open the file in line buffered mode, so we can read along
            if append:
                mode = 'at'
            else:
                mode= 'wt'
            with open(self.logfile, mode, 1) as f:
                sys.stdout = f
                sys.stderr = f
                try:
                    yield
                finally:
                   sys.stdout = stdout
                   sys.stderr = stdout
        else:
            # Nothing to do
            yield

    def _pre_do(self, id=None):
        """Bookkeeping when starting a task."""
        now = datetime.now(utc)
        self.state = 'STARTED'
        self.last_done = now
        if id is None:
            self.last_id = os.getpid()
        else:
            self.last_id = id

    def _do(self):
        """Internal function that does what needs to be done.

        Must be implemented in inheriting classes.
        Retrns `True` on success or `False` when it failed.
        """
        raise NotImplementedError()

    def do(self, id=None):
        """Actually do the task."""

        with self.redirected_output(append=False):
            self._pre_do(id=id)
            print_(self)
            print_("TASK STARTED")
            # Add a timestamp to the beginning of the output
            sh.date(_out=sys.stdout, _tty_out=False)

            try:
                success = self._do()
            except Exception as e:
                # Something went wrong
                self._post_do(state='FAILED', id=id)
                print_("TASK FAILED")
                # Add a timestamp to the end of the output
                sh.date(_out=sys.stdout, _tty_out=False)
                print_(e)
                raise

            if success:
                self._post_do(state='DONE', id=id)
                print_("TASK DONE")
            else:
                self._post_do(state='FAILED', id=id)
                print_("TASK FAILED")
            # Add a timestamp to the end of the output
            sh.date(_out=sys.stdout, _tty_out=False)

        return success

    def _post_do(self, state='DONE', id=None):
        """Bookkeeping when finishing a task."""
        self.state = state
        if id is None:
            self.last_id = os.getpid()
        else:
            self.last_id = id

    def get_due(self):
        """Calculate how due the task is.

        Return value is a float:

            due < 0     -> Task is not due to be done
            due == 0    -> Task should be done excactly now (on the second!)
            due > 0     -> Task is overdue

        The dueness is scaled by the intended period of task execution,
        i.e. a weekly task that has been run 9 days ago is less due than a daily task
        that has last been run 1.5 days ago.
        """

        now = datetime.now(utc)
        day = 24*3600 # seconds per day
        week = 7*day # seconds per week
        month = 30*day # seconds per month
        T = self.get_period()

        # Everything sucks on python 2.6...
        # Define a function that returns the seconds in a timedelta
        sec = lambda td: float(td.seconds + (td.days * day))

        if self.last_done is None:
            # If no last execution is known, assume the task needs to be done since a month ago
            return month / sec(T)
        else:
            return sec(now - (self.last_done + T)) / sec(T)

    def get_logname(self):
        """Get a valid filename that can be used to log the output of the task."""
        return base64.b64encode(self.get_id(), altchars='+_')+'.txt'

    def get_id(self):
        """Return a string that identifies the task."""
        return str(self)

    def __str__(self):
        """Return a string to identify the task by."""
        return '%s_Task'%(self.frequency,)

class CommandTask(Task):
    """General task based on the commands in t2kdm.commands."""

    def __init__(self, **kwargs):
        self.commandline = kwargs.pop('commandline')
        command, argstr = self.commandline.split(' ', 1)
        for cmd in commands.all_commands:
            if cmd.name == command:
                self.command = cmd
                self.argstr = argstr
                break
        else:
            raise ValueError("Unknown command: %s"%(command,))
        super(CommandTask, self).__init__(**kwargs)

    def _do(self):
        # Check interactive command for success return code
        return self.command.run_from_cli(self.argstr, _return=True) == 0

    def __str__(self):
        """Return a string to identify the task by."""
        return '%s_CommandTask_>%s<'%(self.frequency, self.commandline)

class TrimLogTask(Task):
    """Task to trim a log file to a certain number of line.

    The lines will be cut off from the *top*. Equivalent to:

        $ tail -n nlines <path >tempfile
        $ mv tempfile logfile

    """

    def __init__(self, **kwargs):
        """Initialise TrimLogTask.

        Specify the file to be trimmed with the `path` and the number of lines
        to be kept with the `nlines` keyword arguments.
        """

        self.path = kwargs.pop('path')
        self.nlines = kwargs.pop('nlines')
        super(TrimLogTask, self).__init__(**kwargs)

    def _do(self):
        with tempfile.TemporaryFile('w+t') as tf:
            # Write tail of logfile into temporary file
            sh.tail(self.path, lines=self.nlines, _in=self.path, _out=tf, _tty_out=False)
            # Rewind temporary file
            tf.seek(0)
            # Overwrite old file
            sh.cat(_in=tf, _out=self.path, _tty_out=False)
        return True

    def __str__(self):
        """Return a string to identify the task by."""
        return '%s_TrimLogTask_of_>%s<_to_>%s<'%(self.frequency, self.path, self.nlines)

class TaskLog(object):
    """Class to handle the logging of task activity."""

    def __init__(self, filename):
        """Use the given filename as log file."""
        self.filename = filename
        self.timeformat = "%Y-%m-%d_%H:%M:%S%z"
        self.id = os.getpid() # Store an ID to identifiy different processes

    def timestamp(self):
        """Return the current timestamp."""
        time = datetime.now(utc)
        return time.strftime(self.timeformat)

    def log(self, state, task, id=None, end='\n'):
        """Log the STARTED, DONE or FAILED of a task.

        Prepends a timestamp and PID.
        """

        if state not in ['STARTED', 'DONE', 'FAILED']:
            return ValueError("Not a valid task state: %s"%(state,))

        if id is None:
            id = self.id
        with open(self.filename, 'at') as f:
            f.write("%s %s %s %s%s"%(self.timestamp(), id, state, task, end))

    class ParseError(Exception):
        pass

    def parse_time(self, timestamp):
        """Return a datetime object according to the timestamp."""
        timeformat = self.timeformat[:-2] # Need to remove '%z' because pthon <3.2 does not understand it
        timestamp = timestamp[:-5] # Same for timestamp, remove '+0000'
        # We just have to assume here that everything is in UTC
        try:
            dt = datetime.strptime(timestamp, timeformat)
        except ValueError:
            raise TaskLog.ParseError()

        # Make timezone-aware
        dt = dt.replace(tzinfo = utc)
        return dt

    def _parse_line(self, line):
        """Return dict of parsed line."""
        elements = line.split()
        if len(elements) < 4:
            raise TaskLog.ParseError()

        ret = {
            'time': self.parse_time(elements[0]),
            'id': elements[1],
            'state': elements[2],
            'task': ' '.join(elements[3:]),
        }
        return ret

    def parse_log(self):
        """Parse the log file and find the last STARTED, DONE and FAILED times of tasks."""

        last_started = {}
        last_done = {}
        last_failed = {}

        with open(self.filename, 'rt') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#'):
                    # Ignore comments
                    continue

                try: # to parse the line for task run information
                    ret = self._parse_line(line)
                except TaskLog.ParseError:
                    continue

                task = ret['task']
                state = ret['state']
                time = ret['time']
                id = ret['id']

                if state == 'STARTED':
                    # Started a task
                    if task not in last_started or time > last_started[task][1]: # Entry is a tuple of (id, time)
                        last_started[task] = (id, time)
                elif state == 'DONE':
                    # Started a task
                    if task not in last_done or time > last_done[task][1]: # Entry is a tuple of (id, time)
                        last_done[task] = (id, time)
                elif state == 'FAILED':
                    # Started a task
                    if task not in last_failed or time > last_failed[task][1]: # Entry is a tuple of (id, time)
                        last_failed[task] = (id, time)

        return last_started, last_done, last_failed

class Maid(object):
    """Class that deals with all regular data keeping tasks.

    It takes care of replication, checksum checks, and regular reports of the state of things.
    """

    def __init__(self, configfile, report=None):
        """Initialise the Maid with the given configuration file.

        The file must look like this:

            [log]
            tasklog = /path/to/logfile.txt
            trimlog = monthly

            [daily]
            replicate /some/folder/ SOME_SE_disk -vr
            replicate /second/different/folder/to/be/replicated/to/the/SE/@ -vr

            [weekly]
            check /some/folder/ -c -s SOME_SE_disk -vr

            [monthly]
            fix /some/other/folder/@ -vr

        Each command in `daily/weekly/monthly` is handled independently. So it is possible to
        request a weekly transfer of a certain folder, while additionally replicating
        a subfolder every day:

            [daily]
            replicate /some/data/folder/very/important/@ SOME_SE_disk -vr

            [monthly]
            replicate /some/data/folder/ SOME_SE_disk -vr

        If the last level of the path to be replicated is an '@' character,
        it will be replaced by the *lexigraphically* last element in the directory.
        This can be used to define a daily replica of the newest data when the
        folder structure is updated with the run numbers.

        If a `report` folder is specified, the output of the tasks will be redirected
        and a browsable webpage generated in that folder.

        """

        self.report = report

        parser = configparser.SafeConfigParser(allow_no_value=True)
        parser.optionxform = str # Need to make options case sensitive
        parser.read(configfile)

        # Store tasks
        self.tasks = {}

        tasklog_path = parser.get('log', 'tasklog')
        self.tasklog = TaskLog(tasklog_path)

        # Create TrimLogTask
        trim_freq = parser.get('log', 'trimlog')
        new_task = TrimLogTask(path=tasklog_path, nlines=1000, frequency=trim_freq)
        new_id = new_task.get_id()
        # If an html report is requested, redirect the output to the appropriate file
        if self.report is not None:
            new_task.logfile = os.path.join(self.report, new_task.get_logname())

        self.tasks[new_id] = new_task

        for sec in parser.sections():
            freq = sec.lower()
            if freq not in ['daily', 'weekly', 'monthly']:
                continue
            print_("Adding %s tasks..."%(freq,))
            for opt in parser.options(sec):
                val = parser.get(sec, opt) # Create the task from the config file line
                if val is None:
                    print_("Adding task: %s"%(opt,))
                else:
                    print_("Adding task: %s = %s"%(opt,val))
                new_task = CommandTask(commandline=opt, frequency=freq)
                new_id = new_task.get_id()
                if new_id in self.tasks: # Make sure the task does not already exist
                    raise RuntimeError("Duplicate task: %s"%(new_id,))
                # If an html report is requested, redirect the output to the appropriate file
                if self.report is not None:
                    new_task.logfile = os.path.join(self.report, new_task.get_logname())

                # Store task in dict of tasks
                self.tasks[new_id] = new_task

    @staticmethod
    def _quote_html(s):
        trans = html_entities.codepoint2name
        ret = ''.join(trans[x] if x in trans else x for x in s)
        return ret

    def generate_index(self):
        """Generate the index.html page for the reports."""

        if self.report is None:
            # Nothing to do
            return
        else:
            indexfile = os.path.join(self.report, 'index.html')

        # Build the html rows for the tasks
        taskrows = ''
        for name in sorted(self.tasks):
            t = self.tasks[name]
            # Task might never have been run
            if t.last_done is None:
                lastrun = "NEVER"
                state = ""
            else:
                lastrun = t.last_done.strftime(self.tasklog.timeformat)
                state = t.state
                if state == 'STARTED':
                    # Check if the PID is actually still there
                    if not pid_running(int(t.last_id)):
                        state = 'STARTED - PID NOT FOUND'

            taskrows += """
                <tr>
                    <td>{lastrun}</td>
                    <td><a href="{logfile}">{name}</a></td>
                    <td>{state}</td>
                </tr>
            """.format(lastrun = lastrun,
                        logfile = t.get_logname(),
                        state = state,
                        name = self._quote_html(t.get_id()))

        context = {
            'timestamp': self.tasklog.timestamp(),
            'taskrows': taskrows,
        }
        with open(indexfile, 'wt') as f:
            f.write("""<!DOCTYPE html>
                <html lang="en">
                  <head>
                    <meta charset="utf-8">
                    <title>T2K Data Manager - Maid Report</title>
                  </head>
                  <body>
                    <h1>T2K Data Manager - Maid Report</h1>
                    <h2>{timestamp}</h2>
                    <table>
                      {taskrows}
                    </table>
                  </body>
                </html>
            """.format(**context))

    def do_task(self, task):
        """Do a specific task and log it in the tasklog.

        Return `True` if succesfull.
        """

        # Report if necessary
        self.update_task_states() # Make sure tasks are up to date
        task._pre_do() # Set task to STARTED
        self.generate_index()

        # Do the task and log it
        self.tasklog.log('STARTED', task.get_id())
        try:
            success = task.do()
        except KeyboardInterrupt:
            # Catch keyboard interrupts and exit gracefully
            self.tasklog.log('FAILED', task.get_id())
            success = False
        except:
            # No idea what happened
            # Just fail
            self.tasklog.log('FAILED', task.get_id())
            raise
        else:
            # No exceptions, so go by the return value
            if success:
                self.tasklog.log('DONE', task.get_id())
            else:
                self.tasklog.log('FAILED', task.get_id())

        # Report if necessary
        self.update_task_states() # Update tasks that might have run in parallel
        self.generate_index()

        return success

    def update_task_states(self):
        """Read log and update when tasks were last done."""

        started, done, failed = self.tasklog.parse_log()

        for task in started:
            if task in self.tasks:
                t = self.tasks[task]
                if t.last_done is None or t.last_done < started[task][1]:
                    # Do not overwrite start dates that were updated in this process
                    t.last_done = started[task][1] # tuple of (pid, time)
                    t.last_id =  started[task][0]
                    t.state = 'STARTED'

        for task in done:
            if task in self.tasks:
                t = self.tasks[task]
                if t.last_done is None:
                    t.last_done = done[task][1] # tuple of (pid, time)
                    t.last_id =  done[task][0]
                    t.state = 'DONE'
                elif t.last_done <= done[task][1]:
                    # Do not overwrite if task was started after last DONE
                    t.last_id =  done[task][0]
                    t.state = 'DONE'

        for task in failed:
            if task in self.tasks:
                t = self.tasks[task]
                if t.last_done is None:
                    t.last_done = failed[task][1] # tuple of (pid, time)
                    t.last_id =  failed[task][0]
                    t.state = 'FAILED'
                elif t.last_done <= failed[task][1]:
                    # Do not overwrite if task was started after last FAILED
                    t.last_id =  failed[task][0]
                    t.state = 'FAILED'

    def get_open_tasks(self, return_all=False):
        """Return a list of open tasks in order of how due they are.

        If `return_all` is `True`, all tasks will be returned, not just the due ones.
        """

        ret = []
        for t in self.tasks:
            task = self.tasks[t]
            if return_all or task.get_due() >= 0:
                ret.append(task)

        # Sort tasks by dueness
        ret.sort(key=lambda tsk: tsk.get_due(), reverse=True) # Highest due on top
        return ret

    def do_something(self, eager=False):
        """Find an open task and do it.

        If `eager` is `True`, do tasks even before they are due again.
        """

        self.update_task_states()
        tasks = self.get_open_tasks(return_all=eager)

        if len(tasks) > 0:
            print_("Due tasks:")
            for t in tasks:
                print_("* %s (%.3f)"%(t, t.get_due()))

            for t in tasks:
                if t.state == 'STARTED' and pid_running(int(t.last_id)):
                    print_("%s seems to be running already. Skipping..."%(t,))
                    continue
                else:
                    # Found a task we should do
                    break
            else:
                print_("All due tasks seem to be running already. Nothing to do.")
                return

            print_("Starting %s..."%(t))
            if self.do_task(t):
                print_("Done.")
            else:
                print_("Failed.")
        else:
            print_("Nothing to do.")

def run_maid():
    """Start the Maid program and do some tasks.

    Intended to be run multiple times per day, but at least daily.
    """

    parser = argparse.ArgumentParser(description="Regular housekeeping for the T2K data. Run at least daily!")
    parser.add_argument('-e', '--eager', action='store_true',
                        help="do a task, even if it is not due yet")
    parser.add_argument('-r', '--report', metavar='FOLDER', default=None,
                        help="generate an html report in the given folder")
    args = parser.parse_args()

    maid = Maid(dm.config.maid_config, report=args.report)
    maid.do_something(eager=args.eager)

if __name__ == '__main__':
    run_maid()
