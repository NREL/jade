"""Provides ability to run jobs locally or on HPC."""

from collections import OrderedDict
import copy
import datetime
import logging
import os
import shutil

import jade
from jade.common import CONFIG_FILE, JOBS_OUTPUT_DIR, OUTPUT_DIR, \
    RESULTS_FILE, RESULTS_DIR, get_results_temp_filename
from jade.enums import Status
from jade.exceptions import InvalidParameter
from jade.hpc.common import HpcType
from jade.hpc.hpc_manager import HpcManager, AsyncHpcSubmitter
from jade.jobs.job_manager_base import JobManagerBase
from jade.jobs.job_queue import JobQueue
from jade.jobs.job_runner import JobRunner
from jade.jobs.results_aggregator import ResultsAggregatorSummary
from jade.result import serialize_results
from jade.utils.repository_info import RepositoryInfo
from jade.utils.utils import dump_data, load_data, create_script, \
    create_chunks
import jade.version
from jade.utils.timing_utils import timed_debug


logger = logging.getLogger(__name__)

DEFAULTS = {
    "hpc_config_file": "hpc_config.toml",
    "max_nodes": 16,
    "output": OUTPUT_DIR,
    "per_node_batch_size": 500,
    "poll_interval": 30,
}


class JobSubmitter(JobManagerBase):
    """Submits jobs for execution locally or on an HPC."""
    def __init__(self,
                 config_file,
                 hpc_config=DEFAULTS["hpc_config_file"],
                 output=DEFAULTS["output"],
                 ):
        """Constructs JobSubmitter.

        Parameters
        ----------
        config_file : JobConfiguration
            configuration for simulation
        hpc_config : dict, optional
            HPC configuration parameters
            Job timeout in seconds
        output : str
            Output directory

        """
        super(JobSubmitter, self).__init__(config_file, output)
        self._hpc = None
        self._repo_info = None

        master_file = os.path.join(output, CONFIG_FILE)
        shutil.copyfile(config_file, master_file)
        self._config_file = master_file
        logger.debug("Copied %s to %s", config_file, master_file)

        if isinstance(hpc_config, str):
            self._hpc_config_file = hpc_config
        else:
            assert isinstance(hpc_config, dict)
            self._hpc_config_file = os.path.join(self._output,
                                                 "hpc_config.toml")
            dump_data(hpc_config, self._hpc_config_file)

    def __repr__(self):
        return f"""hpc_config_file={self._hpc_config_file}
num_jobs={self.get_num_jobs()}
results_summary={self.get_results_summmary_report()}"""

    def cancel_jobs(self):
        """Cancel running and pending jobs."""
        # TODO

    def create_config_from_failed_jobs(self, output, new_filename):
        """Creates a new configuration file with only parameters that failed
        from the previous run.

        Parameters
        ----------
        output : str
            Output directory for the failing run.
        new_filename : str
            Name of configuration file to create.

        """
        params = self.get_failed_parameters(output)
        new_config = copy.deepcopy(self._config)
        new_config.reconfigure_jobs(params)
        new_config.dump(new_filename)

        logger.info("Created new config file %s with failed jobs from %s",
                    new_filename, output)

    def get_failed_parameters(self, output):
        """Get the parameters from jobs that failed in a previous run.
        The result can be used to reconfigure a JobConfiguration.

        Parameters
        ----------
        output : str
            Output directory for the failing run.

        Returns
        -------
        list of JobParametersInterface stored as namedtuple
            list of namedtuples of parameters

        """
        results_file = os.path.join(output, RESULTS_FILE)
        data = load_data(results_file)
        parameters = []
        for result in data["results"]:
            if result["return_code"] != 0:
                assert False, "TODO broken"
                params = self._config.get_parameters_from_job_name(
                    result["job"])
                parameters.append(params)

            return parameters

    def submit_jobs(self,
                    name="job",
                    per_node_batch_size=DEFAULTS["per_node_batch_size"],
                    max_nodes=DEFAULTS["max_nodes"],
                    force_local=False,
                    verbose=False,
                    poll_interval=DEFAULTS["poll_interval"],
                    num_processes=None):
        """Submit simulations. Auto-detect whether the current system is an HPC
        and submit to its queue. Otherwise, run locally.

        Parameters
        ----------
        name : str
            batch name, applies to HPC job submission only
        per_node_batch_size : int
            Number of jobs to run on one node in one batch.
        max_nodes : int
            Max number of node submission requests to make in parallel.
        force_local : bool
            If on HPC, run jobs through subprocess as if local.
        wait : bool
            Don't return until HPC jobs have finished.
        verbose : bool
            Enable debug logging.
        poll_interval : int
            Inteval in seconds on which to poll jobs.
        num_processes : int
            Number of processes to run in parallel; defaults to num CPUs

        Returns
        -------
        Status

        """
        logger.info("Submit %s jobs for execution.",
                    self._config.get_num_jobs())
        logger.info("JADE version %s", jade.version.__version__)
        self._save_repository_info()

        self._hpc = HpcManager(self._hpc_config_file, self._output)
        result = Status.GOOD

        if self._hpc.hpc_type == HpcType.LOCAL or force_local:
            runner = JobRunner(self._config_file, output=self._output)
            result = runner.run_jobs(
                verbose=verbose, num_processes=num_processes)
        else:
            self._submit_to_hpc(name, max_nodes, per_node_batch_size, verbose,
                                poll_interval, num_processes)

        results_summary = ResultsAggregatorSummary(self._results_dir)
        self._results = results_summary.get_results()
        if len(self._results) != self._config.get_num_jobs():
            logger.error("Number of results doesn't match number of jobs: "
                         "results=%s jobs=%s. Check for process crashes "
                         "or HPC timeouts.",
                         len(self._results), self._config.get_num_jobs())
            result = Status.ERROR

        self.write_results(RESULTS_FILE)
        results_summary.delete_files()
        assert not os.listdir(self._results_dir)
        os.rmdir(self._results_dir)

        return result

    def write_results(self, filename):
        """Write the results to filename in the output directory."""
        data = OrderedDict()
        data["jade_version"] = jade.version.__version__
        now = datetime.datetime.now()
        data["timestamp"] = now.strftime("%m/%d/%Y %H:%M:%S")
        data["base_directory"] = os.getcwd()
        if self._repo_info is not None:
            data["repository_info"] = self._repo_info.summary()
        results = self._build_results()
        data["results_summary"] = results["summary"]
        data["results"] = results["results"]
        data["job_outputs"] = \
            self._config.job_execution_class().collect_results(
                os.path.join(self._output, JOBS_OUTPUT_DIR))

        output_file = os.path.join(self._output, filename)
        dump_data(data, output_file)

        logger.info("Wrote results to %s.", output_file)
        num_failed = results["summary"]["num_failed"]
        log_func = logger.info if num_failed == 0 else logger.warning
        log_func("Successful=%s Failed=%s Total=%s",
                 results["summary"]["num_successful"],
                 num_failed,
                 results["summary"]["total"])

        return output_file

    def _build_results(self):
        num_successful = 0
        num_failed = 0
        for result in self._results:
            if result.return_code == 0 and result.status == "finished":
                num_successful += 1
            else:
                num_failed += 1

        return {
            "results": serialize_results(self._results),
            "summary": {
                "num_successful": num_successful,
                "num_failed": num_failed,
                "total": num_successful + num_failed,
            },
        }

    def _create_run_script(
            self, config_file, filename, num_processes, verbose):
        text = ["#!/bin/bash"]
        if shutil.which("module") is not None:
            # Required for HPC systems.
            text.append("module load conda")
            text.append("conda activate jade")

        command = f"jade-internal run-jobs {config_file} " \
                  f"--output={self._output}"
        if num_processes is not None:
            command += f" --num-processes={num_processes}"
        if verbose:
            command += " --verbose"

        text.append(command)
        create_script(filename, "\n".join(text))

    def _save_repository_info(self):
        try:
            self._repo_info = RepositoryInfo(jade)
            patch = os.path.join(self._output, "diff.patch")
            self._repo_info.write_diff_patch(patch)
            logger.info("Repository information: %s",
                        self._repo_info.summary())
        except InvalidParameter:
            pass

    @timed_debug
    def _split_jobs(self, base_name, batch_size, num_processes, verbose=False):
        """Return a list of AsyncHpcSubmitter objects."""
        submitters = []
        base_config = self._config.serialize()
        jobs = base_config["jobs"]
        for batch in create_chunks(jobs, batch_size):
            new_config = copy.copy(base_config)
            new_config["jobs"] = batch
            index = len(submitters) + 1
            suffix = f"_batch_{index}"
            new_config_file = self._config_file.replace(".json",
                                                        f"{suffix}.json")
            dump_data(new_config, new_config_file)
            logger.info("Created split config file %s with %s jobs",
                        new_config_file, len(new_config["jobs"]))

            run_script = os.path.join(self._output, f"run{suffix}.sh")
            self._create_run_script(
                new_config_file, run_script, num_processes, verbose
            )

            hpc_mgr = HpcManager(self._hpc_config_file, self._output)

            name = base_name + suffix
            hpc_submitter = AsyncHpcSubmitter(hpc_mgr, run_script, name,
                                              self._output)
            submitters.append(hpc_submitter)

        return submitters

    def _submit_to_hpc(self, name, max_nodes, per_node_batch_size, verbose,
                       poll_interval, num_processes):
        queue_depth = max_nodes
        submitters = self._split_jobs(
            name, per_node_batch_size, num_processes, verbose=verbose
        )

        # TODO: this will cause up to 16 slurm status commands every poll.
        # We could send one command, get all statuses, and share it among
        # the submitters.

        JobQueue.run_jobs(
            submitters,
            max_queue_depth=queue_depth,
            poll_interval=poll_interval,
        )
        logger.info("All submitters have completed.")
