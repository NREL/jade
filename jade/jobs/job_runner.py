"""Manages execution of jobs on a node."""

from datetime import datetime
import logging
import os
import shutil
import uuid

from jade.common import OUTPUT_DIR
from jade.enums import Status
from jade.hpc.common import HpcType
from jade.hpc.local_manager import LocalManager
from jade.hpc.pbs_manager import PbsManager
from jade.hpc.slurm_manager import SlurmManager
from jade.jobs.dispatchable_job import DispatchableJob
from jade.jobs.job_manager_base import JobManagerBase
from jade.jobs.job_queue import JobQueue
from jade.result import deserialize_result, serialize_results
from jade.utils.timing_utils import timed_info
from jade.utils.utils import dump_data, load_data, makedirs


logger = logging.getLogger(__name__)


class JobRunner(JobManagerBase):
    """Manages execution of jobs on a node."""

    SUMMARY_FILE_SUFFIX = "summary.toml"

    def __init__(self,
                 config_file,
                 output=OUTPUT_DIR,
                 batch_id=0,
                 ):
        super(JobRunner, self).__init__(config_file, output)

        self._intf, self._intf_type = self._create_node_interface()
        self._batch_id = batch_id

        self._results_suffix = datetime.now().strftime("%Y%m%d_%H%M%S") + \
            f"_batch_{batch_id}"

        logger.debug("Constructed JobRunner output=%s batch=%s", output,
                     batch_id)

    @timed_info
    def run_jobs(self, verbose=False):
        """Run the jobs.

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
            result = self._run_jobs(jobs)
            self._consolidate_results()

            assert len(self._results) == self._config.get_num_jobs(), \
                f"{len(self._results)} {self._config.get_num_jobs()}"
            logger.info("Completed %s jobs", len(self._results))
        finally:
            shutil.rmtree(scratch_dir)

        return result

    def _consolidate_results(self):
        logger.debug("Collect results in %s %s suffix=%s", self._results_dir,
                     self._batch_id, self._results_suffix)
        result_files = []
        self._results.clear()

        for filename in os.listdir(self._results_dir):
            if filename.endswith(f"{self._results_suffix}.toml"):
                path = os.path.join(self._results_dir, filename)
                result_files.append(path)
                result = deserialize_result(load_data(path))
                self._results.append(result)
                logger.debug("Deserialized job result %s", path)

        suffix = self.SUMMARY_FILE_SUFFIX
        summary = os.path.join(
            self._results_dir, f"results_{self._results_suffix}_{suffix}"
        )
        dump_data({"results": serialize_results(self._results)}, summary)
        logger.info("Wrote summary of job batch to %s", summary)

        for result_file in result_files:
            logger.debug("Removing temp results file %s", result_file)
            os.remove(result_file)

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
        return [
            DispatchableJob(
                job,
                job_exec_class.generate_command(
                    job, self._jobs_output, config_file, verbose=verbose),
                self._output
            ) for job in self._config.iter_jobs()
        ]

    def _run_jobs(self, jobs):
        num_jobs = len(jobs)
        max_num_workers = self._intf.get_num_cpus()
        num_workers = min(num_jobs, max_num_workers)
        logger.info("Generated %s jobs to execute on %s workers max=%s.",
                    num_jobs, num_workers, max_num_workers)
        self._intf.log_environment_variables()

        for job in jobs:
            job.set_results_filename_suffix(self._results_suffix)

        # TODO: make this non-blocking so that we can report status.
        JobQueue.run_jobs(jobs, max_queue_depth=num_workers)

        logger.info("Jobs are complete. count=%s", num_jobs)
        return Status.GOOD  # TODO
