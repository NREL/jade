"""Defines an async CLI command."""

import logging
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

from jade.common import JOBS_OUTPUT_DIR, JOBS_STDIO_DIR, RESULTS_DIR
from jade.enums import JobCompletionStatus, Status
from jade.events import StructuredLogEvent, EVENT_NAME_BYTES_CONSUMED, EVENT_CATEGORY_RESOURCE_UTIL
from jade.jobs.async_job_interface import AsyncJobInterface
from jade.jobs.results_aggregator import ResultsAggregator
from jade.loggers import log_event
from jade.result import Result
from jade.utils.utils import get_directory_size_bytes


logger = logging.getLogger(__name__)


class AsyncCliCommand(AsyncJobInterface):
    """Defines a a CLI command that can be submitted asynchronously."""

    def __init__(self, job, cmd, output, batch_id, is_manager_node, hpc_job_id):
        self._job = job
        self._cli_cmd = cmd
        self._output = Path(output)
        self._pipe = None
        self._is_pending = False
        self._start_time = None
        self._return_code = None
        self._is_complete = False
        self._batch_id = batch_id
        self._is_manager_node = is_manager_node
        self._hpc_job_id = hpc_job_id
        self._stdout_fp = None
        self._stderr_fp = None

    def __del__(self):
        if self._is_pending:
            logger.warning("job %s destructed while pending", self._cli_cmd)

    def _complete(self):
        self._return_code = self._pipe.returncode
        self._stdout_fp.close()
        self._stderr_fp.close()
        exec_time_s = time.time() - self._start_time

        if not self._is_manager_node:
            # This will happen on a multi-node job. Don't complete it multiple times.
            logger.info(
                "Job %s completed on non-manager node return_code=%s exec_time_s=%s",
                self._job.name,
                self._return_code,
                exec_time_s,
            )
            return

        status = JobCompletionStatus.FINISHED
        output_dir = self._output / JOBS_OUTPUT_DIR / self._job.name
        bytes_consumed = get_directory_size_bytes(output_dir)
        event = StructuredLogEvent(
            source=self._job.name,
            category=EVENT_CATEGORY_RESOURCE_UTIL,
            name=EVENT_NAME_BYTES_CONSUMED,
            message="job output directory size",
            bytes_consumed=bytes_consumed,
        )
        log_event(event)
        result = Result(
            self._job.name, self._return_code, status, exec_time_s, hpc_job_id=self._hpc_job_id
        )
        ResultsAggregator.append(self._output, result, batch_id=self._batch_id)

        logger.info(
            "Job %s completed return_code=%s exec_time_s=%s hpc_job_id=%s",
            self._job.name,
            self._return_code,
            exec_time_s,
            self._hpc_job_id,
        )

    def cancel(self):
        self._return_code = 1
        self._is_complete = True
        if self._is_manager_node:
            result = Result(
                self._job.name,
                self._return_code,
                JobCompletionStatus.CANCELED,
                0.0,
                hpc_job_id=self._hpc_job_id,
            )
            ResultsAggregator.append(self._output, result, batch_id=self._batch_id)
            logger.info("Canceled job %s", self._job.name)
        else:
            logger.info("Canceled job %s on non-manager node", self._job.name)

    @property
    def cancel_on_blocking_job_failure(self):
        return self._job.cancel_on_blocking_job_failure

    def get_id(self):
        return self._pipe.pid

    def set_blocking_jobs(self, jobs):
        self._job.set_blocking_jobs(jobs)

    def is_complete(self):
        if self._is_complete:
            return True

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
        """Get the job.

        Parameters
        ----------
        job : JobParametersInterface

        """
        return self._job

    @property
    def name(self):
        return self._job.name

    def get_blocking_jobs(self):
        return self._job.get_blocking_jobs()

    def remove_blocking_job(self, name):
        self._job.remove_blocking_job(name)

    @property
    def return_code(self):
        return self._return_code

    def run(self):
        """Run the job. Writes results to file when complete."""
        assert self._pipe is None
        self._start_time = time.time()

        # Disable posix if on Windows.
        cmd = shlex.split(self._cli_cmd, posix="win" not in sys.platform)
        env = os.environ.copy()
        env["JADE_JOB_NAME"] = self.name
        stdout_filename = self._output / JOBS_STDIO_DIR / f"{self._job.name}.o"
        stderr_filename = self._output / JOBS_STDIO_DIR / f"{self._job.name}.e"
        self._stdout_fp = open(stdout_filename, "w")
        self._stderr_fp = open(stderr_filename, "w")
        self._pipe = subprocess.Popen(cmd, env=env, stdout=self._stdout_fp, stderr=self._stderr_fp)
        self._is_pending = True
        logger.info("Started job name=%s hpc_job_id=%s", self._job.name, self._hpc_job_id)
        logger.debug("Submitted %s", self._cli_cmd)
        return Status.GOOD
