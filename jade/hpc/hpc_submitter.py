"""Controls submission of jobs to HPC nodes."""

import copy
import logging
import os
import re
import sys
import time
from datetime import timedelta
from pathlib import Path

from jade.enums import JobCompletionStatus, Status
from jade.events import (
    StructuredLogEvent,
    EVENT_CATEGORY_HPC,
    EVENT_NAME_HPC_SUBMIT,
    EVENT_NAME_HPC_JOB_ASSIGNED,
    EVENT_NAME_HPC_JOB_STATE_CHANGE,
)
from jade.hpc.common import HpcJobStatus, HpcType
from jade.hpc.hpc_manager import HpcManager
from jade.jobs.async_job_interface import AsyncJobInterface
from jade.jobs.cluster import Cluster
from jade.jobs.job_configuration import JobConfiguration
from jade.jobs.job_queue import JobQueue
from jade.jobs.results_aggregator import ResultsAggregator
from jade.loggers import log_event
from jade.models import JobState
from jade.models.submission_group import make_submission_group_lookup
from jade.result import Result
from jade.utils.timing_utils import timed_debug
from jade.utils.utils import dump_data, create_script, ExtendedJSONEncoder

logger = logging.getLogger(__name__)


class HpcSubmitter:
    """Submits batches of jobs to HPC. Manages job ordering."""

    LOCK_FILENAME = "submitter.lock"

    def __init__(self, config: JobConfiguration, config_file, cluster: Cluster, output):
        self._config = config
        self._submission_groups = make_submission_group_lookup(cluster.config.submission_groups)
        self._config_file = config_file
        self._base_config = config.serialize()
        self._batch_index = cluster.job_status.batch_index
        self._cluster = cluster
        self._hpc_mgr = HpcManager(self._submission_groups, output)
        self._output = output

        # Limitation: these settings apply to all groups in aggregate.
        # This could be made more flexible if needed.
        group = next(iter(self._submission_groups.values()))
        self._max_nodes = group.submitter_params.max_nodes
        if self._max_nodes is None:
            self._max_nodes = sys.maxsize
        self._poll_interval = group.submitter_params.poll_interval
        self._status_collector = HpcStatusCollector(self._hpc_mgr, self._poll_interval)

    def _create_run_script(self, config_file, filename, submission_group):
        text = ["#!/bin/bash"]
        command = f"jade-internal run-jobs {config_file} " f"--output={self._output}"
        if submission_group.submitter_params.num_processes is not None:
            command += f" --num-processes={submission_group.submitter_params.num_processes}"
        if submission_group.submitter_params.verbose:
            command += " --verbose"

        text.append(command)
        create_script(filename, "\n".join(text))

    def _make_async_submitter(self, jobs, submission_group, dry_run=False):
        config = copy.copy(self._base_config)
        config["jobs"] = jobs
        suffix = f"_batch_{self._batch_index}"
        self._batch_index += 1
        new_config_file = self._config_file.replace(".json", f"{suffix}.json")
        dump_data(config, new_config_file, cls=ExtendedJSONEncoder)
        logger.info(
            "Created split config file %s with %s jobs", new_config_file, len(config["jobs"])
        )

        run_script = os.path.join(self._output, f"run{suffix}.sh")
        self._create_run_script(new_config_file, run_script, submission_group)

        name = submission_group.submitter_params.hpc_config.job_prefix + suffix
        return AsyncHpcSubmitter(
            self._hpc_mgr,
            self._status_collector,
            run_script,
            name,
            submission_group,
            self._output,
            dry_run=dry_run,
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
        # TODO: consider whether we need to save the real job names
        hpc_submitters = [
            AsyncHpcSubmitter.create_from_id(self._hpc_mgr, self._status_collector, x)
            for x in self._cluster.iter_hpc_job_ids()
        ]

        queue = JobQueue(
            self._max_nodes,
            existing_jobs=hpc_submitters,
            poll_interval=self._poll_interval,
        )
        # Statuses may have changed since we last ran.
        # Persistent network errors could cause this submitter to fail.
        # Another submitter will try again later (unless this is the last submitter).
        queue.process_queue()
        completed_job_names, canceled_jobs = self._update_completed_jobs()

        lock_file = Path(self._output) / self.LOCK_FILENAME
        assert not lock_file.exists(), lock_file
        lock_file.touch()

        # Start submitting jobs. If any unexpected exception prevents us from updating the
        # status file, leave the lock_file in place and intentionally cause a deadlock.
        try:
            blocked_jobs = []
            submitted_jobs = []
            for group in self._cluster.config.submission_groups:
                if not queue.is_full():
                    self._submit_batches(queue, group, blocked_jobs, submitted_jobs)

            num_submissions = self._batch_index - starting_batch_index
            logger.info(
                "num_batches=%s num_submitted=%s num_blocked=%s new_completions=%s",
                num_submissions,
                len(submitted_jobs),
                len(blocked_jobs),
                len(completed_job_names),
            )

            hpc_job_ids = sorted([x.job_id for x in queue.outstanding_jobs])
            self._update_status(
                submitted_jobs,
                blocked_jobs,
                canceled_jobs,
                hpc_job_ids,
                completed_job_names,
            )

            is_complete = self._is_complete()
            os.remove(lock_file)
            return is_complete
        except Exception:
            logger.exception(
                "An exception occurred while the submitter was active. "
                "The state of the cluster is unknown. A deadlock will occur."
            )
            raise

    def _update_status(
        self, submitted_jobs, blocked_jobs, canceled_jobs, hpc_job_ids, completed_job_names
    ):
        hpc_job_changes = self._cluster.job_status.hpc_job_ids != hpc_job_ids
        if completed_job_names or submitted_jobs or blocked_jobs or hpc_job_changes:
            self._cluster.update_job_status(
                submitted_jobs,
                blocked_jobs,
                canceled_jobs,
                completed_job_names,
                hpc_job_ids,
                self._batch_index,
            )

    def _submit_batches(self, queue, submission_group, blocked_jobs, submitted_jobs):
        assert not queue.is_full()
        num_submitted_jobs = 0
        _submitted_jobs = []  # only exists for the check at the end
        available_jobs = self._get_available_jobs(submission_group)
        while not queue.is_full() and available_jobs:
            batch, available_jobs = self._make_batch(
                available_jobs, submission_group, _submitted_jobs, blocked_jobs
            )
            if batch.num_jobs > 0:
                self._submit_batch(queue, submission_group, batch)
                num_submitted_jobs += batch.num_jobs

        assert num_submitted_jobs == len(
            _submitted_jobs
        ), f"{num_submitted_jobs} / {len(_submitted_jobs)}"
        submitted_jobs.extend(_submitted_jobs)

    def _get_available_jobs(self, submission_group):
        available_jobs = []
        for job in self._cluster.iter_jobs(state=JobState.NOT_SUBMITTED):
            jade_job = self._config.get_job(job.name)
            if jade_job.submission_group == submission_group.name:
                available_jobs.append(job)
        return available_jobs

    def _make_batch(self, available_jobs, submission_group, submitted_jobs, blocked_jobs):
        blocked_jobs_by_name = {}
        submitted_jobs_by_name = {}
        batch = _BatchJobs(submission_group.submitter_params)
        if submission_group.submitter_params.try_add_blocked_jobs:
            # Allow multiple rounds in case the user listed blocked jobs before
            # their blocking jobs in the config file.
            max_iterations = len(available_jobs)
        else:
            max_iterations = 1
        highest_index = -1
        done = False
        for _ in range(max_iterations):
            for i, job in enumerate(available_jobs):
                if i > highest_index:
                    highest_index = i
                if job.name in submitted_jobs_by_name:
                    continue
                jade_job = self._config.get_job(job.name)
                if batch.is_job_blocked(job):
                    blocked_jobs_by_name[job.name] = job
                else:
                    jade_job.set_blocking_jobs(job.blocked_by)
                    batch.append(jade_job)
                    submitted_jobs.append(job)
                    submitted_jobs_by_name[job.name] = job
                    blocked_jobs_by_name.pop(job.name, None)
                if batch.ready_to_submit() or len(submitted_jobs_by_name) == len(available_jobs):
                    done = True
                    break
            if done:
                break

        for job in blocked_jobs_by_name.values():
            blocked_jobs.append(job)
        if highest_index == len(available_jobs) - 1:
            not_checked = []
        else:
            not_checked = available_jobs[highest_index + 1 :]
        return batch, not_checked

    def _submit_batch(self, queue, submission_group, batch):
        async_submitter = self._make_async_submitter(
            batch.serialize(), submission_group, dry_run=submission_group.submitter_params.dry_run
        )
        queue.submit(async_submitter)
        self._log_submission_event(submission_group, batch)

    def _is_complete(self):
        is_complete = self._cluster.are_all_jobs_complete()
        if not is_complete and not self._cluster.job_status.hpc_job_ids:
            # TODO: need to implement persistent recording of fake status
            if self._hpc_mgr.hpc_type != HpcType.FAKE:
                logger.error(
                    "Some jobs are not complete but there are no active HPC job IDs. "
                    "Force completion."
                )
                is_complete = True

        return is_complete

    def _log_submission_event(self, submission_group, batch):
        event = StructuredLogEvent(
            source=submission_group.submitter_params.hpc_config.job_prefix,
            category=EVENT_CATEGORY_HPC,
            name=EVENT_NAME_HPC_SUBMIT,
            message="Submitted HPC batch",
            batch_size=batch.num_jobs,
            per_node_batch_size=submission_group.submitter_params.per_node_batch_size,
        )
        log_event(event)

    def _cancel_job(self, job):
        job.state = JobState.DONE
        job.blocked_by.clear()
        result = Result(job.name, 1, JobCompletionStatus.CANCELED, 0)
        ResultsAggregator.append(self._output, result)
        logger.info("Canceled job %s because one of its blocking jobs failed.", job.name)

    def _update_completed_jobs(self):
        newly_completed = set()
        canceled_jobs = []
        # If jobs fail and are configured to cancel blocked jobs, we may need to run this
        # loop many times to cancel the entire chain.
        need_to_rerun = True
        while need_to_rerun:
            need_to_rerun = False
            aggregator = ResultsAggregator.load(self._output)
            failed_jobs = set()
            for result in aggregator.process_results():
                newly_completed.add(result.name)
                if result.return_code != 0:
                    failed_jobs.add(result.name)

            logger.debug("Detected completion of jobs: %s", newly_completed)
            logger.debug("Detected failed jobs: %s", failed_jobs)
            for job in self._cluster.iter_jobs(state=JobState.NOT_SUBMITTED):
                if job.blocked_by:
                    if job.cancel_on_blocking_job_failure and job.blocked_by.intersection(
                        failed_jobs
                    ):
                        self._cancel_job(job)
                        canceled_jobs.append(job)
                        need_to_rerun = True
                    else:
                        job.blocked_by.difference_update(newly_completed)

        return newly_completed, canceled_jobs


class _BatchJobs:
    """Helper class to manage jobs in a batch."""

    def __init__(self, params):
        self._estimated_batch_time = timedelta(seconds=0)
        self._num_processes = params.num_processes
        self._per_node_batch_size = params.per_node_batch_size
        self._time_based_batching = params.time_based_batching
        self._try_add_blocked_jobs = params.try_add_blocked_jobs
        self._jobs = []
        self._job_names = set()
        if self._time_based_batching:
            self._max_batch_time = (
                _to_timedelta(params.hpc_config.hpc.walltime) * self._num_processes
            )
        else:
            self._max_batch_time = None

    def append(self, job):
        """Append a job."""
        self._jobs.append(job)
        self._job_names.add(job.name)
        if self._time_based_batching:
            self._estimated_batch_time += timedelta(minutes=job.estimated_run_minutes)

    def are_blocking_jobs_present(self, blocking_jobs):
        """Return True if all blocking jobs are already in the batch.

        Returns
        -------
        bool

        """
        return blocking_jobs.issubset(self._job_names)

    def ready_to_submit(self):
        """Return True if the batch has enough jobs to submit."""
        if self._time_based_batching:
            if self._estimated_batch_time >= self._max_batch_time:
                return True
        elif self.num_jobs >= self._per_node_batch_size:
            return True
        return False

    def is_job_blocked(self, job):
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
        if self._try_add_blocked_jobs and self.are_blocking_jobs_present(job.blocked_by):
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

    def __init__(
        self,
        hpc_manager,
        status_collector,
        run_script,
        name,
        submission_group,
        output,
        job_id=None,
        dry_run=False,
    ):
        self._mgr = hpc_manager
        self._status_collector = status_collector
        self._run_script = run_script
        self._submission_group = submission_group
        self._job_id = job_id
        self._output = output
        self._name = name
        # Re-enable this if we ever keep a submitter active and polling again.
        # self._last_status = HpcJobStatus.NONE
        self._is_complete = False
        self._dry_run = dry_run
        self._return_code = None

    def cancel(self):
        self._mgr.cancel_job(self._job_id)

    @property
    def cancel_on_blocking_job_failure(self):
        return False

    @classmethod
    def create_from_id(cls, hpc_manager, status_collector, job_id):
        """Create an instance of a job_id in order to check status.

        Parameters
        ----------
        job_id : str

        """
        return cls(hpc_manager, status_collector, None, job_id, None, None, job_id=job_id)

    @property
    def hpc_manager(self):
        """Return the HpcManager object.

        Returns
        -------
        HpcManager

        """
        return self._mgr

    def is_complete(self):
        if self._is_complete:
            return self._is_complete
        status = self._status_collector.check_status(self._job_id)
        # if status != self._last_status:
        logger.info("HPC job ID %s status=%s", self._job_id, status)
        #    event = StructuredLogEvent(
        #        source=self._name,
        #        category=EVENT_CATEGORY_HPC,
        #        name=EVENT_NAME_HPC_JOB_STATE_CHANGE,
        #        message="HPC job state change",
        #        job_id=self._job_id,
        #        old_state=self._last_status.value,
        #        new_state=status.value,
        #    )
        #    log_event(event)
        #    self._last_status = status

        self._is_complete = status in (HpcJobStatus.COMPLETE, HpcJobStatus.NONE)
        return self._is_complete

    @property
    def job_id(self):
        return self._job_id

    @property
    def name(self):
        return self._name

    @property
    def return_code(self):
        assert self._is_complete
        return self._return_code

    def run(self):
        assert self._submission_group is not None
        job_id, result = self._mgr.submit(
            self._output,
            self._name,
            self._run_script,
            self._submission_group.name,
            dry_run=self._dry_run,
        )
        if result != Status.GOOD:
            # TODO: cancel or fail all jobs in the batch
            logger.error("Failed to submit name=%s", self._name)
            self._return_code = 1
            self._is_complete = True
            return Status.ERROR

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
        return Status.GOOD

    def get_blocking_jobs(self):
        return set()

    def remove_blocking_job(self, name):
        assert False

    def set_blocking_jobs(self, jobs):
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

        Raises
        ------
        ExecutionError
            Raised if statuses cannot be retrieved.

        """
        cur_time = time.time()
        if self._last_poll_time is None or cur_time - self._last_poll_time > self._poll_interval:
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


_REGEX_WALL_TIME = re.compile(r"(\d+):(\d+):(\d+)")


def _to_timedelta(wall_time):
    match = _REGEX_WALL_TIME.search(wall_time)
    assert match
    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3))
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)
