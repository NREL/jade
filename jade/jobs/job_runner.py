"""Manages execution of jobs on a node."""

import logging
import os
import shutil
import time
import uuid

from jade.common import JOBS_OUTPUT_DIR, OUTPUT_DIR, get_temp_results_filename
from jade.enums import Status, ResourceMonitorType
from jade.hpc.common import HpcType
from jade.hpc.hpc_manager import HpcManager
from jade.jobs.cluster import Cluster
from jade.jobs.async_cli_command import AsyncCliCommand
from jade.jobs.job_manager_base import JobManagerBase
from jade.jobs.job_queue import JobQueue
from jade.models import JobState
from jade.loggers import setup_logging
from jade.resource_monitor import ResourceMonitorAggregator, ResourceMonitorLogger
from jade.utils.timing_utils import timed_info


logger = logging.getLogger(__name__)


class JobRunner(JobManagerBase):
    """Manages execution of jobs on a node."""

    def __init__(
        self,
        config_file,
        output,
        batch_id=0,
    ):
        super(JobRunner, self).__init__(config_file, output)
        cluster, _ = Cluster.deserialize(output)
        self._check_jobs(cluster)
        self._handle_submission_groups_after_deserialize(cluster)
        group = self.config.get_default_submission_group()
        config = group.submitter_params.hpc_config
        self._intf = HpcManager.create_hpc_interface(config)
        self._node_id = self._intf.get_node_id()
        self._intf_type = config.hpc_type
        self._batch_id = batch_id
        self._event_file = os.path.join(
            output,
            f"run_jobs_batch_{batch_id}_{self._node_id}_events.log",
        )
        self._event_logger = None

        logger.debug("Constructed JobRunner output=%s batch=%s", output, batch_id)

    def _check_jobs(self, cluster: Cluster):
        if cluster.job_status is None:
            # This is true in local mode and these checks are not relevant.
            return

        # Perform a sanity check. If all jobs aren't "submitted" then something went wrong.
        submitted_jobs = {job.name for job in cluster.iter_jobs(state=JobState.SUBMITTED)}
        error_jobs = []
        for job in self._config.iter_jobs():
            if job.name not in submitted_jobs:
                error_jobs.append(job.name)
        if error_jobs:
            logger.error("Jobs were not in the submitted state: %s", " ".join(error_jobs))
            assert not error_jobs, f"number of jobs not in submitted state = {len(error_jobs)}"

    @property
    def node_id(self):
        """Return the node ID of the current system.

        Returns
        -------
        str

        """
        return self._node_id

    @timed_info
    def run_jobs(self, verbose=False, num_processes=None):
        """Run the jobs.

        Parameters
        ----------
        verbose : bool
            If True, enable debug logging.
        num_processes : int
            Number of processes to run in parallel; defaults to num CPUs

        Returns
        -------
        Status

        """
        logger.info("Run jobs.")
        scratch_dir = self._create_local_scratch()
        are_inputs_local = self._intf_type == HpcType.LOCAL

        # Not sure exactly why, but this needs to come after run_jobs.py has initialized logging.
        self._event_logger = setup_logging(
            "event", self._event_file, console_level=logging.ERROR, file_level=logging.INFO
        )

        try:
            config_file = self._config.serialize_for_execution(scratch_dir, are_inputs_local)

            jobs = self._generate_jobs(config_file, verbose)
            result = self._run_jobs(jobs, num_processes=num_processes)
            logger.info("Completed %s jobs", len(jobs))
        finally:
            shutil.rmtree(scratch_dir)
            if not are_inputs_local:
                self._complete_hpc_job()

        return result

    def _complete_hpc_job(self):
        """Complete the HPC job in the cluster. A future submitter could detect this. However,
        it's better to do it here for these reasons:

        1. If we don't do this and the user runs 'jade show-status -j' before the next submitter
           runs, a stale HPC job ID will show up.
        2. The last submitter will never detect its job as complete, and so the job status file
           will always include it.

        """
        job_id = self._intf.get_current_job_id()
        if job_id is None:
            # TODO: need to implement persistent recording of fake status for FakeManager
            return

        max_time_s = 30
        interval = 5
        completed = False
        for _ in range(max_time_s // interval):
            cluster, promoted = Cluster.deserialize(
                self._output, try_promote_to_submitter=True, deserialize_jobs=True
            )
            if promoted:
                try:
                    cluster.complete_hpc_job_id(job_id)
                finally:
                    cluster.demote_from_submitter()
                completed = True
                break
            time.sleep(interval)

        if not completed:
            logger.warning(
                "Could not promote to submitter. HPC job ID %s is still present", job_id
            )
            # Couldn't acquire lock because other nodes were continually doing work (unexpected).
            # A future submitter will see that this job ID is complete and clear the condition.

    def _create_local_scratch(self):
        local_scratch = self._intf.get_local_scratch()
        dirname = "jade-" + str(uuid.uuid4())
        scratch_dir = os.path.join(local_scratch, dirname)
        os.makedirs(scratch_dir, exist_ok=True)
        logger.info("Created jade scratch_dir=%s", scratch_dir)
        return scratch_dir

    def _generate_jobs(self, config_file, verbose):
        jobs = []
        for job in self._config.iter_jobs():
            job_exec_class = self._config.job_execution_class(job.extension)
            djob = AsyncCliCommand(
                job,
                job_exec_class.generate_command(
                    job,
                    self._jobs_output,
                    config_file,
                    verbose=verbose,
                ),
                self._output,
            )
            jobs.append(djob)

        return jobs

    def _run_jobs(self, jobs, num_processes=None):
        num_jobs = len(jobs)
        if num_processes is None:
            max_num_workers = self._intf.get_num_cpus()
        else:
            max_num_workers = num_processes
        num_workers = min(num_jobs, max_num_workers)
        logger.info(
            "Generated %s jobs to execute on %s workers max=%s.",
            num_jobs,
            num_workers,
            max_num_workers,
        )
        self._intf.log_environment_variables()

        name = f"resource_monitor_batch_{self._batch_id}_{self._node_id}"
        group = self._config.get_default_submission_group()
        monitor_type = group.submitter_params.resource_monitor_type
        resource_aggregator = None
        if monitor_type == ResourceMonitorType.AGGREGATION:
            resource_aggregator = ResourceMonitorAggregator(name)
            monitor_func = resource_aggregator.update_resource_stats
            resource_monitor_interval = group.submitter_params.resource_monitor_interval
        elif monitor_type == ResourceMonitorType.PERIODIC:
            resource_monitor = ResourceMonitorLogger(name)
            monitor_func = resource_monitor.log_resource_stats
            resource_monitor_interval = group.submitter_params.resource_monitor_interval
        elif monitor_type == ResourceMonitorType.NONE:
            monitor_func = None
            resource_monitor_interval = None
        else:
            assert False, monitor_type
        JobQueue.run_jobs(
            jobs,
            max_queue_depth=num_workers,
            monitor_func=monitor_func,
            monitor_interval=resource_monitor_interval,
        )

        logger.info("Jobs are complete. count=%s", num_jobs)
        if resource_aggregator is not None:
            resource_aggregator.finalize(self._output)
        self._aggregate_events()
        return Status.GOOD  # TODO

    @timed_info
    def _aggregate_events(self):
        # Aggregate all job events.log files into this node's log file so
        # that the we can more quickly make events.json later.
        for handler in self._event_logger.handlers:
            handler.close()
        with open(self._event_file, "a") as f_out:
            for job in self._config.iter_jobs():
                job_file = os.path.join(self._output, JOBS_OUTPUT_DIR, job.name, "events.log")
                if not os.path.exists(job_file):
                    # Extensions aren't required to create these.
                    continue
                with open(job_file) as f_in:
                    for line in f_in:
                        f_out.write(line)
                os.remove(job_file)
                logger.debug("Moved contents of %s to %s", job_file, self._event_file)
