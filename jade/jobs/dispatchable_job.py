"""Defines a dispatchable job."""

import logging
import shlex
import subprocess
import sys
import time

from jade.jobs.dispatchable_job_interface import DispatchableJobInterface
from jade.jobs.results_aggregator import ResultsAggregator
from jade.result import Result


logger = logging.getLogger(__name__)


class DispatchableJob(DispatchableJobInterface):
    """Defines a dispatchable job."""
    def __init__(self, job, cmd, output, results_filename):
        self._job = job
        self._cli_cmd = cmd
        self._output = output
        self._pipe = None
        self._results_filename = results_filename
        self._is_pending = False
        self._start_time = None

    def __del__(self):
        if self._is_pending:
            logger.warning("job %s destructed while pending", self._cli_cmd)

    def _complete(self):
        ret = self._pipe.returncode
        exec_time_s = time.time() - self._start_time

        job_filename = self._job.name
        illegal_chars = ("/", "\\", ":")
        for char in illegal_chars:
            job_filename = job_filename.replace(char, "-")

        status = "finished"
        # TODO: the only reliable way to get storage space consumed on Lustre
        # may be to sum the size of each created file here and generate an
        # event.
        result = Result(self._job.name, ret, status, exec_time_s)
        ResultsAggregator.append(self._results_filename, result)

        logger.info("Job %s completed return_code=%s exec_time_s=%s",
                    self._job.name, ret, exec_time_s)

    def is_complete(self):
        if not self._is_pending:
            ret = self._pipe.poll()
            assert ret is None, f"{ret}"
            return True

        if self._pipe.poll() is not None:
            self._is_pending = False
            self._complete()

        return not self._is_pending

    @property
    def job(self):
        return self._job

    @property
    def name(self):
        return self._job.name

    def get_blocking_jobs(self):
        return self._job.get_blocking_jobs()

    def remove_blocking_job(self, name):
        self._job.remove_blocking_job(name)

    def run(self):
        """Run the job. Writes results to file when complete."""
        assert self._pipe is None
        self._start_time = time.time()

        # Disable posix if on Windows.
        cmd = shlex.split(self._cli_cmd, posix="win" not in sys.platform)
        self._pipe = subprocess.Popen(cmd)
        self._is_pending = True
        logger.debug("Submitted %s", self._cli_cmd)
