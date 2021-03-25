"""Defines a dispatchable job."""

import logging
import os
import shlex
import subprocess
import sys
import time

from jade.common import JOBS_OUTPUT_DIR
from jade.events import StructuredLogEvent, EVENT_NAME_BYTES_CONSUMED, \
    EVENT_CATEGORY_RESOURCE_UTIL
from jade.jobs.dispatchable_job_interface import DispatchableJobInterface
from jade.jobs.results_aggregator import ResultsAggregator
from jade.loggers import log_event
from jade.result import Result
from jade.utils.utils import get_directory_size_bytes


logger = logging.getLogger(__name__)


class DispatchableJob(DispatchableJobInterface):
    """Defines a dispatchable job."""
    def __init__(self, job, cmd, output, results_filename):
        self._job = job
        self._cli_cmd = cmd
        self._output = output
        self._pipe = None
        self._is_pending = False
        self._start_time = None

    def __del__(self):
        if self._is_pending:
            logger.warning("job %s destructed while pending", self._cli_cmd)

    def _complete(self):
        ret = self._pipe.returncode
        exec_time_s = time.time() - self._start_time

        status = "finished"
        output_dir = os.path.join(self._output, JOBS_OUTPUT_DIR, self._job.name)
        bytes_consumed = get_directory_size_bytes(output_dir)
        event = StructuredLogEvent(
            source=self._job.name,
            category=EVENT_CATEGORY_RESOURCE_UTIL,
            name=EVENT_NAME_BYTES_CONSUMED,
            message="job output directory size",
            bytes_consumed=bytes_consumed,
        )
        log_event(event)
        result = Result(self._job.name, ret, status, exec_time_s)
        ResultsAggregator.append(self._output, result)

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
