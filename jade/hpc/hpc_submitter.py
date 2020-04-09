"""Controls submission of jobs to HPC nodes."""

import copy
import logging
import os
import shutil
import time

from jade.enums import Status
from jade.events import StructuredEvent, EVENT_CATEGORY_HPC, \
    EVENT_CODE_HPC_SUBMIT, EVENT_CODE_HPC_JOB_ASSIGNED, \
    EVENT_CODE_HPC_JOB_STATE_CHANGE
from jade.exceptions import ExecutionError
from jade.hpc.common import HpcJobStatus
from jade.hpc.hpc_manager import HpcManager
from jade.jobs.async_job_interface import AsyncJobInterface
from jade.jobs.job_queue import JobQueue
from jade.jobs.results_aggregator import ResultsAggregatorSummary
from jade.loggers import log_job_event
from jade.utils.timing_utils import timed_debug
from jade.utils.utils import dump_data, create_script

logger = logging.getLogger(__name__)


class HpcSubmitter:
    """Submits batches of jobs to HPC. Manages job ordering."""
    def __init__(self, name, config, config_file, hpc_config_file, results_dir):
        self._config = config
        self._config_file = config_file
        self._hpc_config_file = hpc_config_file
        self._base_config = config.serialize()
        self._name = name
        self._batch_index = 1
        self._results_summary = ResultsAggregatorSummary(results_dir)

    @staticmethod
    def _create_run_script(config_file, filename, num_processes, output, verbose):
        text = ["#!/bin/bash"]
        if shutil.which("module") is not None:
            # Required for HPC systems.
            text.append("module load conda")
            text.append("conda activate jade")

        command = f"jade-internal run-jobs {config_file} " \
                  f"--output={output}"
        if num_processes is not None:
            command += f" --num-processes={num_processes}"
        if verbose:
            command += " --verbose"

        text.append(command)
        create_script(filename, "\n".join(text))

    def _make_async_submitter(self, jobs, num_processes, output, verbose):
        config = copy.copy(self._base_config)
        config["jobs"] = jobs
        suffix = f"_batch_{self._batch_index}"
        self._batch_index += 1
        new_config_file = self._config_file.replace(".json", f"{suffix}.json")
        dump_data(config, new_config_file)
        logger.info("Created split config file %s with %s jobs",
                    new_config_file, len(config["jobs"]))

        run_script = os.path.join(output, f"run{suffix}.sh")
        self._create_run_script(
            new_config_file, run_script, num_processes, output, verbose
        )

        hpc_mgr = HpcManager(self._hpc_config_file, output)
        name = self._name + suffix
        return AsyncHpcSubmitter(hpc_mgr, run_script, name, output)

    @timed_debug
    def run(self, output, queue_depth, per_node_batch_size, num_processes,
            poll_interval=60, verbose=False):
        """Run all jobs defined in the configuration on the HPC."""
        queue = JobQueue(queue_depth, poll_interval=poll_interval)
        jobs = list(self._config.iter_jobs())
        while jobs:
            self._update_completed_jobs(jobs)
            batch_jobs = []
            jobs_to_pop = []
            num_blocked = 0
            for i, job in enumerate(jobs):
                if job.get_blocking_jobs():
                    num_blocked += 1
                else:
                    batch_jobs.append(job.serialize())
                    jobs_to_pop.append(i)
                    if len(batch_jobs) >= per_node_batch_size:
                        break

            if batch_jobs:
                async_submitter = self._make_async_submitter(
                    batch_jobs,
                    num_processes,
                    output,
                    verbose,
                )

                queue.submit(async_submitter)

                # It might be better to delay submission for a limited number
                # of rounds if there are blocked jobs and the batch isn't full.
                # We can look at these events on our runs to see how this
                # logic is working with our jobs.
                event = StructuredEvent(
                    name=self._name,
                    category=EVENT_CATEGORY_HPC,
                    code=EVENT_CODE_HPC_SUBMIT,
                    message="Submitted HPC batch",
                    batch_size=len(batch_jobs),
                    num_blocked=num_blocked,
                    per_node_batch_size=per_node_batch_size,
                )
                log_job_event(event)
                for i in reversed(jobs_to_pop):
                    jobs.pop(i)
            else:
                logger.debug("No jobs are ready for submission")

            logger.debug("num_submitted=%s num_blocked=%s",
                         len(batch_jobs), num_blocked)

            if not queue.is_full():
                # Keep submitting.
                continue

            # TODO: this will cause up to <queue_depth> slurm status commands
            # every poll.  We could send one command, get all statuses, and
            # share it among the submitters.
            queue.process_queue()
            time.sleep(poll_interval)

        queue.wait()

    def _is_job_complete(self, job_name):
        return job_name in self._results_summary.completed_jobs

    def _update_completed_jobs(self, jobs):
        self._results_summary.update_completed_jobs()
        for job in jobs:
            done_jobs = [
                x for x in job.get_blocking_jobs() if self._is_job_complete(x)
            ]
            for name in done_jobs:
                job.remove_blocking_job(name)


class AsyncHpcSubmitter(AsyncJobInterface):
    """Used to submit batches of jobs to multiple nodes, one at a time."""
    def __init__(self, hpc_manager, run_script, name, output):
        self._mgr = hpc_manager
        self._run_script = run_script
        self._job_id = None
        self._output = output
        self._name = name
        self._last_status = HpcJobStatus.NONE
        self._is_pending = False

    def __del__(self):
        if self._is_pending:
            logger.warning("job %s destructed while pending", self._name)

    @property
    def hpc_manager(self):
        """Return the HpcManager object.

        Returns
        -------
        HpcManager

        """
        return self._mgr

    def is_complete(self):
        status = self._mgr.check_status(job_id=self._job_id)

        if status != self._last_status:
            logger.info("Submission %s %s changed status from %s to %s",
                        self._name, self._job_id, self._last_status, status)
            event = StructuredEvent(
                name=self._name,
                category=EVENT_CATEGORY_HPC,
                code=EVENT_CODE_HPC_JOB_STATE_CHANGE,
                message="HPC job state change",
                job_id=self._job_id,
                old_state=self._last_status.value,
                new_state=status.value,
            )
            log_job_event(event)
            self._last_status = status

        if status in (HpcJobStatus.COMPLETE, HpcJobStatus.NONE):
            self._is_pending = False

        return not self._is_pending

    def name(self):
        return self._name

    def run(self):
        job_id, result = self._mgr.submit(self._output,
                                          self._name,
                                          self._run_script)
        self._is_pending = True
        if result != Status.GOOD:
            raise ExecutionError("Failed to submit name={self._name}")

        self._job_id = job_id
        event = StructuredEvent(
            name=self._name,
            category=EVENT_CATEGORY_HPC,
            code=EVENT_CODE_HPC_JOB_ASSIGNED,
            message="HPC job assigned",
            job_id=self._job_id,
        )
        log_job_event(event)
        logger.info("Assigned job_ID=%s name=%s", self._job_id, self._name)

    def get_blocking_jobs(self):
        return []

    def remove_blocking_job(self, name):
        assert False
