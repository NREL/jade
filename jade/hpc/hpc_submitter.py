"""Controls submission of jobs to HPC nodes."""

import copy
import logging
import os
import shutil
import time

from jade.enums import Status
from jade.events import StructuredLogEvent, EVENT_CATEGORY_HPC, \
    EVENT_NAME_HPC_SUBMIT, EVENT_NAME_HPC_JOB_ASSIGNED, \
    EVENT_NAME_HPC_JOB_STATE_CHANGE
from jade.exceptions import ExecutionError
from jade.hpc.common import HpcJobStatus
from jade.hpc.hpc_manager import HpcManager
from jade.jobs.async_job_interface import AsyncJobInterface
from jade.jobs.job_queue import JobQueue
from jade.jobs.results_aggregator import ResultsAggregatorSummary
from jade.loggers import log_event
from jade.utils.timing_utils import timed_debug
from jade.utils.utils import dump_data, create_script, ExtendedJSONEncoder

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

    def _make_async_submitter(self, jobs, hpc_mgr, status_collector, num_processes, output, verbose):
        config = copy.copy(self._base_config)
        config["jobs"] = jobs
        suffix = f"_batch_{self._batch_index}"
        self._batch_index += 1
        new_config_file = self._config_file.replace(".json", f"{suffix}.json")
        dump_data(config, new_config_file, cls=ExtendedJSONEncoder)
        logger.info("Created split config file %s with %s jobs",
                    new_config_file, len(config["jobs"]))

        run_script = os.path.join(output, f"run{suffix}.sh")
        self._create_run_script(
            new_config_file, run_script, num_processes, output, verbose
        )

        name = self._name + suffix
        return AsyncHpcSubmitter(hpc_mgr, status_collector, run_script, name, output)

    @timed_debug
    def run(self, output, queue_depth, per_node_batch_size, num_processes,
            poll_interval=60, try_add_blocked_jobs=False, verbose=False):
        """Run all jobs defined in the configuration on the HPC."""
        queue = JobQueue(queue_depth, poll_interval=poll_interval)
        hpc_mgr = HpcManager(self._hpc_config_file, output)
        status_collector = HpcStatusCollector(hpc_mgr, poll_interval)
        jobs = list(self._config.iter_jobs())
        while jobs:
            self._update_completed_jobs(jobs)
            batch = _BatchJobs()
            jobs_to_pop = []
            num_blocked = 0
            for i, job in enumerate(jobs):
                if batch.is_job_blocked(job, try_add_blocked_jobs):
                    num_blocked += 1
                else:
                    batch.append(job)
                    jobs_to_pop.append(i)
                    if batch.num_jobs >= per_node_batch_size:
                        break

            if batch.num_jobs > 0:
                async_submitter = self._make_async_submitter(
                    batch.serialize(),
                    hpc_mgr,
                    status_collector,
                    num_processes,
                    output,
                    verbose,
                )
                queue.submit(async_submitter)

                # It might be better to delay submission for a limited number
                # of rounds if there are blocked jobs and the batch isn't full.
                # We can look at these events on our runs to see how this
                # logic is working with our jobs.
                event = StructuredLogEvent(
                    source=self._name,
                    category=EVENT_CATEGORY_HPC,
                    name=EVENT_NAME_HPC_SUBMIT,
                    message="Submitted HPC batch",
                    batch_size=batch.num_jobs,
                    num_blocked=num_blocked,
                    per_node_batch_size=per_node_batch_size,
                )
                log_event(event)
                for i in reversed(jobs_to_pop):
                    jobs.pop(i)
            else:
                logger.debug("No jobs are ready for submission")

            logger.debug("num_submitted=%s num_blocked=%s",
                         batch.num_jobs, num_blocked)

            if batch.num_jobs > 0 and not queue.is_full():
                # Keep submitting.
                continue

            queue.process_queue()
            time.sleep(poll_interval)

        queue.wait()

        # Sanity check
        statuses = status_collector.get_statuses()
        if statuses:
            for status in statuses:
                assert status in (HpcJobStatus.COMPLETE, HpcJobStatus.NONE)

    def _update_completed_jobs(self, jobs):
        self._results_summary.update_completed_jobs()
        for job in jobs:
            done_jobs = [
                x for x in job.get_blocking_jobs()
                if x in self._results_summary.completed_jobs
            ]
            for name in done_jobs:
                job.remove_blocking_job(name)


class _BatchJobs:
    """Helper class to manage jobs in a batch."""
    def __init__(self):
        self._jobs = []
        self._job_names = set()

    def append(self, job):
        """Append a job."""
        self._jobs.append(job)
        self._job_names.add(job.name)

    def are_blocking_jobs_present(self, blocking_jobs):
        """Return True if all blocking jobs are already in the batch.

        Returns
        -------
        bool

        """
        return blocking_jobs.issubset(self._job_names)

    def is_job_blocked(self, job, try_add_blocked_jobs):
        """Return True if the job is blocked."""
        blocking_jobs = job.get_blocking_jobs()
        if not blocking_jobs:
            return False
        if try_add_blocked_jobs and self.are_blocking_jobs_present(blocking_jobs):
            # JobRunner will manage the execution ordering on the compute node.
            return False
        return True

    @property
    def num_jobs(self):
        """Return the number of jobs in the batch."""
        return len(self._jobs)

    def serialize(self):
        """Serialize all jobs in the batch.

        Returns
        -------
        list
            list of dict

        """
        return [x.serialize() for x in self._jobs]


class AsyncHpcSubmitter(AsyncJobInterface):
    """Used to submit batches of jobs to multiple nodes, one at a time."""
    def __init__(self, hpc_manager, status_collector, run_script, name, output):
        self._mgr = hpc_manager
        self._status_collector = status_collector
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
        status = self._status_collector.check_status(self._job_id)
        if status != self._last_status:
            logger.info("Submission %s %s changed status from %s to %s",
                        self._name, self._job_id, self._last_status, status)
            event = StructuredLogEvent(
                source=self._name,
                category=EVENT_CATEGORY_HPC,
                name=EVENT_NAME_HPC_JOB_STATE_CHANGE,
                message="HPC job state change",
                job_id=self._job_id,
                old_state=self._last_status.value,
                new_state=status.value,
            )
            log_event(event)
            self._last_status = status

        if status in (HpcJobStatus.COMPLETE, HpcJobStatus.NONE):
            self._is_pending = False

        return not self._is_pending

    @property
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
        event = StructuredLogEvent(
            source=self._name,
            category=EVENT_CATEGORY_HPC,
            name=EVENT_NAME_HPC_JOB_ASSIGNED,
            message="HPC job assigned",
            job_id=self._job_id,
        )
        log_event(event)
        logger.info("Assigned job_ID=%s name=%s", self._job_id, self._name)

    def get_blocking_jobs(self):
        return set()

    def remove_blocking_job(self, name):
        assert False


class HpcStatusCollector:
    """Collects status for all user jobs."""
    def __init__(self, hpc_mgr, poll_interval):
        self._hpc_mgr = hpc_mgr
        self._poll_interval = poll_interval
        self._last_poll_time = None
        self._statuses = {}

    def check_status(self, job_id):
        """Return the status for job_id.

        Parameters
        ----------
        job_id : str

        Returns
        -------
        HpcJobStatus

        """
        cur_time = time.time()
        if self._last_poll_time is None or \
                cur_time - self._last_poll_time > self._poll_interval:
            logger.debug("Collect new statuses.")
            self._statuses = self._hpc_mgr.check_statuses()
            self._last_poll_time = cur_time

        return self._statuses.get(job_id, HpcJobStatus.NONE)

    def get_statuses(self):
        """Return outstanding statuses

        Returns
        -------
        list
            list of HpcJobStatus

        """
        return list(self._statuses.values())
