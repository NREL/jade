"""Manages execution of jobs on a node."""

import logging
import os
import shutil
import time
import uuid
import time
from pathlib import Path

from jade.common import JOBS_OUTPUT_DIR, OUTPUT_DIR
from jade.enums import Status, ResourceMonitorType
from jade.exceptions import ExecutionError
from jade.hpc.common import HpcType
from jade.hpc.hpc_manager import HpcManager
from jade.jobs.cluster import Cluster
from jade.jobs.async_cli_command import AsyncCliCommand
from jade.jobs.job_manager_base import JobManagerBase
from jade.jobs.job_queue import JobQueue
from jade.models import JobState
from jade.loggers import close_event_logging
from jade.resource_monitor import ResourceMonitorAggregator, ResourceMonitorLogger
from jade.utils.run_command import run_command, check_run_command
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
        self._handle_submission_groups()
        group = self.config.get_default_submission_group()
        config = group.submitter_params.hpc_config
        self._intf = HpcManager.create_hpc_interface(config)
        self._node_id = self._intf.get_node_id()
        self._intf_type = config.hpc_type
        self._batch_id = batch_id
        self._event_filename = os.path.join(
            output,
            f"run_jobs_batch_{batch_id}_{self._node_id}_events.log",
        )
        self._event_logger = None

        logger.debug("Constructed JobRunner output=%s batch=%s", output, batch_id)

    @property
    def event_filename(self):
        return self._event_filename

    @property
    def node_id(self):
        """Return the node ID of the current system.

        Returns
        -------
        str

        """
        return self._node_id

    @timed_info
    def run_jobs(
        self, distributed_submitter=True, verbose=False, num_parallel_processes_per_node=None
    ):
        """Run the jobs.

        Parameters
        ----------
        distributed_submitter : bool
            If True, make cluster updates.
        verbose : bool
            If True, enable debug logging.
        num_parallel_processes_per_node : int
            Number of processes to run in parallel; defaults to num CPUs

        Returns
        -------
        Status

        """
        logger.info("Run jobs.")
        scratch_dir = self._create_local_scratch()
        are_inputs_local = self._intf_type == HpcType.LOCAL

        try:
            config_file = self._config.serialize_for_execution(scratch_dir, are_inputs_local)
            jobs = self._generate_jobs(config_file, verbose)

            os.environ["JADE_RUNTIME_OUTPUT"] = self._output
            os.environ["JADE_SUBMISSION_GROUP"] = self._config.get_default_submission_group().name
            # Setting node_setup_script and node_shutdown_script are obsolete and will
            # eventually be deleted.
            group = self._config.get_default_submission_group()
            if group.submitter_params.node_setup_script is not None:
                cmd = f"{group.submitter_params.node_setup_script} {config_file} {self._output}"
                check_run_command(cmd)
            elif self._config.node_setup_command is not None:
                check_run_command(self._config.node_setup_command)

            result = self._run_jobs(
                jobs, num_parallel_processes_per_node=num_parallel_processes_per_node
            )

            if group.submitter_params.node_shutdown_script:
                cmd = f"{group.submitter_params.node_shutdown_script} {config_file} {self._output}"
                ret2 = run_command(cmd)
                if ret2 != 0:
                    logger.error("Failed to run node shutdown script %s: %s", cmd, ret2)
            elif self._config.node_teardown_command is not None:
                start = time.time()
                ret2 = run_command(self._config.node_teardown_script)
                if ret2 != 0:
                    logger.error(
                        "Failed to run node shutdown script %s: %s",
                        self._config.node_teardown_command,
                        ret2,
                    )
                logger.info("Node teardown script duration = %s seconds", time.time() - start)

            logger.info("Completed %s jobs", len(jobs))
        finally:
            shutil.rmtree(scratch_dir)
            if distributed_submitter and are_inputs_local:
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
                self._batch_id,
                self._intf.am_i_manager(),
                self._intf.get_current_job_id(),
            )
            jobs.append(djob)

        return jobs

    def _run_jobs(self, jobs, num_parallel_processes_per_node=None):
        num_jobs = len(jobs)
        if num_parallel_processes_per_node is None:
            max_num_workers = self._intf.get_num_cpus()
        else:
            max_num_workers = num_parallel_processes_per_node
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
            resource_aggregator = ResourceMonitorAggregator(
                name, group.submitter_params.resource_monitor_stats
            )
            monitor_func = resource_aggregator.update_resource_stats
            resource_monitor_interval = group.submitter_params.resource_monitor_interval
        elif monitor_type == ResourceMonitorType.PERIODIC:
            resource_monitor = ResourceMonitorLogger(
                name, group.submitter_params.resource_monitor_stats
            )
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
            poll_interval=group.submitter_params.poll_interval,
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
        close_event_logging()
        with open(self._event_filename, "a") as f_out:
            for job in self._config.iter_jobs():
                job_file = os.path.join(self._output, JOBS_OUTPUT_DIR, job.name, "events.log")
                if not os.path.exists(job_file):
                    # Extensions aren't required to create these.
                    continue
                with open(job_file) as f_in:
                    for line in f_in:
                        f_out.write(line)
                os.remove(job_file)
                logger.debug("Moved contents of %s to %s", job_file, self._event_filename)
