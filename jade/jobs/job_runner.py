"""Manages execution of jobs on a node."""

import logging
import os
import shutil
import uuid

from jade.common import OUTPUT_DIR, get_results_temp_filename
from jade.enums import Status
from jade.hpc.common import HpcType
from jade.hpc.local_manager import LocalManager
from jade.hpc.pbs_manager import PbsManager
from jade.hpc.slurm_manager import SlurmManager
from jade.jobs.dispatchable_job import DispatchableJob
from jade.jobs.job_manager_base import JobManagerBase
from jade.jobs.job_post_process import JobPostProcess
from jade.jobs.job_queue import JobQueue
from jade.jobs.results_aggregator import ResultsAggregator
from jade.utils.timing_utils import timed_info
from jade.utils.utils import makedirs


logger = logging.getLogger(__name__)


class JobRunner(JobManagerBase):
    """Manages execution of jobs on a node."""
    def __init__(self,
                 config_file,
                 output=OUTPUT_DIR,
                 batch_id=0,
                 ):
        super(JobRunner, self).__init__(config_file, output)

        self._intf, self._intf_type = self._create_node_interface()
        self._batch_id = batch_id

        logger.debug("Constructed JobRunner output=%s batch=%s", output,
                     batch_id)

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

        try:
            config_file = self._config.serialize_for_execution(
                scratch_dir, are_inputs_local)

            jobs = self._generate_jobs(config_file, verbose)
            result = self._run_jobs(jobs, num_processes=num_processes)
            # run post process
            self._run_post_process(verbose)
            logger.info("Completed %s jobs", len(jobs))
        finally:
            shutil.rmtree(scratch_dir)

        return result

    def _create_local_scratch(self):
        local_scratch = self._intf.get_local_scratch()
        dirname = "jade-" + str(uuid.uuid4())
        scratch_dir = os.path.join(local_scratch, dirname)
        makedirs(scratch_dir)
        logger.info("Created jade scratch_dir=%s", scratch_dir)
        return scratch_dir

    @staticmethod
    def _create_node_interface():
        """Returns an interface implementation appropriate for the current
        environment.

        """
        cluster = os.environ.get("NREL_CLUSTER")
        # These will not be used, but are required.
        config = {"hpc": {"allocation": None, "walltime": None}}
        if cluster is None:
            intf = LocalManager(config)
            intf_type = HpcType.LOCAL
        elif cluster == "peregrine":
            intf = PbsManager(config)
            intf_type = HpcType.PBS
        elif cluster == "eagle":
            intf = SlurmManager(config)
            intf_type = HpcType.SLURM
        else:
            raise ValueError("Unsupported node type: {}".format(cluster))

        logger.debug("node manager type=%s", intf_type)
        return intf, intf_type

    def _generate_jobs(self, config_file, verbose):
        job_exec_class = self._config.job_execution_class()
        results_filename = get_results_temp_filename(
            self._output, self._batch_id
        )
        results_aggregator = ResultsAggregator(results_filename)
        results_aggregator.create_file()

        return [
            DispatchableJob(
                job,
                job_exec_class.generate_command(
                    job, self._jobs_output, config_file, verbose=verbose),
                self._output,
                results_filename
            ) for job in self._config.iter_jobs()
        ]

    def _run_jobs(self, jobs, num_processes=None):
        num_jobs = len(jobs)
        if num_processes is None:
            max_num_workers = self._intf.get_num_cpus()
        else:
            max_num_workers = num_processes
        num_workers = min(num_jobs, max_num_workers)
        logger.info("Generated %s jobs to execute on %s workers max=%s.",
                    num_jobs, num_workers, max_num_workers)
        self._intf.log_environment_variables()

        # TODO: make this non-blocking so that we can report status.
        JobQueue.run_jobs(jobs, max_queue_depth=num_workers)

        logger.info("Jobs are complete. count=%s", num_jobs)
        return Status.GOOD  # TODO

    def _run_post_process(self, verbose):
        """Runs post process function, if given"""
        post_process_config = self._config.post_process_config
        if post_process_config is None:
            return

        logger.info("Running post-process %s", post_process_config['class_name'])
        post_process = JobPostProcess(*post_process_config.values())
        post_process.run(self._config_file)
