"""Controls submission of jobs to HPC nodes."""

import copy
import logging
import os
import shutil
import time

from jade.enums import Status
from jade.events import (
    StructuredLogEvent, EVENT_CATEGORY_HPC, EVENT_NAME_HPC_SUBMIT,
    EVENT_NAME_HPC_JOB_ASSIGNED, EVENT_NAME_HPC_JOB_STATE_CHANGE
)
from jade.exceptions import ExecutionError
from jade.hpc.common import HpcJobStatus
from jade.hpc.hpc_manager import HpcManager
from jade.jobs.async_job_interface import AsyncJobInterface
from jade.jobs.job_queue import JobQueue
from jade.jobs.cluster import Cluster
from jade.jobs.cluster import Cluster
from jade.loggers import log_event
from jade.models import ClusterConfig, JobState
from jade.utils.timing_utils import timed_debug
from jade.utils.utils import dump_data, create_script, ExtendedJSONEncoder

logger = logging.getLogger(__name__)


class HpcSubmitter:
    """Submits batches of jobs to HPC. Manages job ordering."""
    def __init__(self, config, config_file, results_dir, cluster, output):
        self._config = config
        self._config_file = config_file
        self._base_config = config.serialize()
        self._batch_index = cluster.job_status.batch_index
        self._completion_detector = CompletionDetector(results_dir)
        self._cluster = cluster
        self._params = self._cluster.config.submitter_params
        self._hpc_mgr = HpcManager(self._params.hpc_config, output)
        self._status_collector = HpcStatusCollector(self._hpc_mgr, self._params.poll_interval)
        self._output = output

    def _create_run_script(self, config_file, filename):
        text = ["#!/bin/bash"]
        command = f"jade-internal run-jobs {config_file} " \
                  f"--output={self._output}"
        if self._params.num_processes is not None:
            command += f" --num-processes={self._params.num_processes}"
        if self._params.verbose:
            command += " --verbose"

        text.append(command)
        create_script(filename, "\n".join(text))

    def _make_async_submitter(self, jobs):
        config = copy.copy(self._base_config)
        config["jobs"] = jobs
        suffix = f"_batch_{self._batch_index}"
        self._batch_index += 1
        new_config_file = self._config_file.replace(".json", f"{suffix}.json")
        dump_data(config, new_config_file, cls=ExtendedJSONEncoder)
        logger.info("Created split config file %s with %s jobs",
                    new_config_file, len(config["jobs"]))

        run_script = os.path.join(self._output, f"run{suffix}.sh")
        self._create_run_script(new_config_file, run_script)

        name = self._params.hpc_config.job_prefix + suffix
        return AsyncHpcSubmitter(
            self._hpc_mgr,
            self._status_collector,
            run_script,
            name,
            self._output,
        )

    @timed_debug
    def run(self):
        """Try to submit batches of jobs to the HPC.

        Returns
        -------
        bool
            Returns True if all jobs are complete.

        """
        starting_batch_index = self._batch_index
        try_add_blocked_jobs = self._cluster.config.submitter_params.try_add_blocked_jobs
        # TODO: consider whether we need to save the real job names
        hpc_submitters = [
            AsyncHpcSubmitter.create_from_id(self._hpc_mgr, self._status_collector, x)
            for x in self._cluster.iter_hpc_job_ids()
        ]

        queue = JobQueue(
            self._params.max_nodes,
            existing_jobs=hpc_submitters,
            poll_interval=self._params.poll_interval,
        )
        # Statuses may have changed since we last ran.
        queue.process_queue()
        hpc_job_ids = sorted([x.job_id for x in queue.outstanding_jobs])
        completed_job_names = self._update_completed_jobs()

        blocked_jobs = []
        submitted_jobs = []
        batch = None
        for job in self._cluster.iter_jobs(state=JobState.NOT_SUBMITTED):
            if batch is None:
                batch = _BatchJobs()
            jade_job = self._config.get_job(job.name)
            if batch.is_job_blocked(job, try_add_blocked_jobs):
                blocked_jobs.append(job)
            else:
                jade_job.set_blocking_jobs(job.blocked_by)
                batch.append(jade_job)
                submitted_jobs.append(job)

            if batch is not None and batch.num_jobs >= self._params.per_node_batch_size:
                self._submit_batch(queue, batch, hpc_job_ids)
                batch = None

            if queue.is_full():
                break

        if batch is not None and batch.num_jobs > 0:
            self._submit_batch(queue, batch, hpc_job_ids)

        num_submissions = self._batch_index - starting_batch_index
        logger.info("num_batches=%s num_submitted=%s num_blocked=%s new_completions=%s",
                    num_submissions, len(submitted_jobs), len(blocked_jobs), len(completed_job_names))
        if submitted_jobs:
            logger.debug("Submitted %s", ", ".join((x.name for x in submitted_jobs)))

        self._update_status(submitted_jobs, blocked_jobs, hpc_job_ids, completed_job_names)
        return self._cluster.are_all_jobs_complete()

    def _update_status(self, submitted_jobs, blocked_jobs, hpc_job_ids, completed_job_names):
        hpc_job_changes = self._cluster.job_status.hpc_job_ids != hpc_job_ids
        if completed_job_names or submitted_jobs or blocked_jobs or hpc_job_changes:
            self._cluster.update_job_status(
                submitted_jobs,
                blocked_jobs,
                completed_job_names,
                hpc_job_ids,
                self._batch_index,
            )

    def _submit_batch(self, queue, batch, hpc_job_ids):
        async_submitter = self._make_async_submitter(batch.serialize())
        queue.submit(async_submitter)
        hpc_job_ids.append(async_submitter.job_id)
        self._log_submission_event(batch)

    def _log_submission_event(self, batch):
        event = StructuredLogEvent(
            source=self._params.hpc_config.job_prefix,
            category=EVENT_CATEGORY_HPC,
            name=EVENT_NAME_HPC_SUBMIT,
            message="Submitted HPC batch",
            batch_size=batch.num_jobs,
            per_node_batch_size=self._params.per_node_batch_size,
        )
        log_event(event)

    def _update_completed_jobs(self):
        newly_completed = self._completion_detector.update_completed_jobs()
        all_completed_jobs = self._completion_detector.completed_jobs
        for job in self._cluster.iter_jobs(state=JobState.NOT_SUBMITTED):
            if job.blocked_by:
                job.blocked_by.difference_update(all_completed_jobs)

        return newly_completed


class CompletionDetector:
    """Detects when individual jobs complete to release blocked jobs."""
    def __init__(self, path):
        self._path = path
        self._completed_jobs = set()

    @property
    def completed_jobs(self):
        """Return the completed jobs.

        Returns
        -------
        set

        """
        return self._completed_jobs

    def update_completed_jobs(self):
        """Check for completed jobs."""
        newly_completed = []
        for filename in os.listdir(self._path):
            logger.debug("Detected completion of job=%s", filename)
            self._completed_jobs.add(filename)
            os.remove(os.path.join(self._path, filename))
            newly_completed.append(filename)

        return newly_completed


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
        """Return True if the job is blocked.

        Parameters
        ----------
        job : Job

        Returns
        -------
        bool

        """
        if not job.blocked_by:
            return False
        if try_add_blocked_jobs and self.are_blocking_jobs_present(job.blocked_by):
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
    def __init__(self, hpc_manager, status_collector, run_script, name, output, job_id=None):
        self._mgr = hpc_manager
        self._status_collector = status_collector
        self._run_script = run_script
        self._job_id = job_id
        self._output = output
        self._name = name
        self._last_status = HpcJobStatus.NONE
        self._is_pending = False

    @classmethod
    def create_from_id(cls, hpc_manager, status_collector, job_id):
        """Create an instance of a job_id in order to check status.

        Parameters
        ----------
        job_id : str

        """
        return cls(hpc_manager, status_collector, None, job_id, None, job_id=job_id)

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
    def job_id(self):
        return self._job_id

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
