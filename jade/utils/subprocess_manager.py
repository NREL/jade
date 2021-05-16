"""Functionality to execute non-blocking commands in a subprocess."""

import logging
import shlex
import subprocess
import sys
import threading
import time

from jade.exceptions import JobAlreadyInProgress

# These functions used to be in this file. Leave this here for backwards compatibility.
from .run_command import run_command, check_run_command


logger = logging.getLogger(__name__)


class SubprocessManager:
    """Allows non-blocking execution of a command in a subprocess."""

    def __init__(self):
        self._pipe = None
        self._return_code = None
        self._shutdown = False
        self._thread = None

    def _run_worker(self, cmd, timeout):
        if timeout is None:
            # Effectively, no timeout.
            timeout = sys.maxsize

        self._return_code = None
        start = time.time()
        self._pipe = subprocess.Popen(shlex.split(cmd))

        ret = None
        elapsed_time = 0
        sleep_interval = 1

        while not self._shutdown and ret is None and elapsed_time < timeout:
            time.sleep(sleep_interval)
            elapsed_time += sleep_interval
            ret = self._pipe.poll()

        exec_time = time.time() - start
        self._return_code = ret

        if self._shutdown:
            logger.info("Exit job thread for shutdown")
            self._terminate()
        elif self._return_code is None:
            logger.error("Command [%s] timed out, terminate it; timeout=%s", cmd, timeout)
            self._terminate()
        else:
            logger.info(
                "Command [%s] completed return_code=%s " "exec_time_s=%s",
                cmd,
                self._return_code,
                exec_time,
            )

        self._pipe = None

    def _terminate(self):
        """Should only be called by execution thread."""
        assert threading.get_ident() == self._thread.ident

        if self._pipe is not None:
            self._pipe.terminate()
        # else it may have been terminated before actually starting the cmd.

    def in_progress(self):
        """Returns True if commands are still running."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def return_code(self):
        """Return the return coe of the last completed command.

        Returns
        -------
        int | None
            Returns None if the command did not complete.

        """
        return self._return_code

    def run(self, command, timeout=None):
        """Run a command without blocking. Call wait_for_completion() or
        in_progress() to monitor progress.

        Parameters
        ----------
        command : str
            Command to execute
        timeout: int or None
            Timeout in seconds for each command. None means no timeout.

        Raises
        ------
        JobAlreadyInProgress
            Raised if execution is already in progress.

        """
        if self.in_progress():
            raise JobAlreadyInProgress(
                "SubprocessManager only supports " "execution of one job at a time."
            )

        logger.debug("Start command execution thread.")

        self._thread = threading.Thread(
            target=self._run_worker, args=(command, timeout), name="run_command"
        )

        # The thread (and subprocesses) will get killed if the main process
        # is killed.
        self._thread.daemon = True

        self._thread.start()

    def terminate(self):
        """Terminates the command."""
        logger.info("Terminate execution.")
        self._shutdown = True
        self.wait_for_completion()

    def wait_for_completion(self):
        """Waits until the command is complete.

        Returns
        -------
        int | None
            return code if the command completed, otherwise None

        """
        try:
            self._thread.join()
            return self._return_code
        except KeyboardInterrupt:
            logger.info("Detected Ctrl-C: command is still outstanding")
            self._shutdown = True
            return None
