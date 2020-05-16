"""Provides ability to run jobs locally or on HPC."""

from collections import OrderedDict
import datetime
import fileinput
import importlib
import logging
import os
import shutil

import jade
from jade.common import CONFIG_FILE, JOBS_OUTPUT_DIR, OUTPUT_DIR, \
    RESULTS_FILE
from jade.enums import Status
from jade.events import EVENTS_FILENAME, EVENT_NAME_ERROR_LOG, \
    StructuredLogEvent, EVENT_CATEGORY_ERROR, EVENT_CATEGORY_RESOURCE_UTIL, \
    EVENT_NAME_BYTES_CONSUMED
from jade.exceptions import InvalidParameter
from jade.extensions.registry import Registry, ExtensionClassType
from jade.hpc.common import HpcType
from jade.hpc.hpc_manager import HpcManager
from jade.hpc.hpc_submitter import HpcSubmitter
from jade.jobs.job_manager_base import JobManagerBase
from jade.jobs.job_runner import JobRunner
from jade.jobs.results_aggregator import ResultsAggregatorSummary
from jade.loggers import log_event
from jade.result import serialize_results
from jade.utils.repository_info import RepositoryInfo
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import dump_data, get_directory_size_bytes
import jade.version


logger = logging.getLogger(__name__)

DEFAULTS = {
    "hpc_config_file": "hpc_config.toml",
    "max_nodes": 16,
    "output": OUTPUT_DIR,
    "per_node_batch_size": 500,
    "poll_interval": 30,
    "bpp_max_nodes": 1,
    "bpp_per_node_batch_size": 500,
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

    def submit_jobs(self,
                    name="job",
                    per_node_batch_size=DEFAULTS["per_node_batch_size"],
                    max_nodes=DEFAULTS["max_nodes"],
                    force_local=False,
                    verbose=False,
                    poll_interval=DEFAULTS["poll_interval"],
                    num_processes=None,
                    previous_results=None,
                    reports=True):
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
        registry = Registry()
        loggers = registry.list_loggers()
        logger.info("Registered modules for logging: %s", ", ".join(loggers))
        self._save_repository_info(registry)

        self._config.check_job_dependencies()

        self._hpc = HpcManager(self._hpc_config_file, self._output)
        result = Status.GOOD

        # If an events summary file exists, it is invalid.
        events_file = os.path.join(self._output, EVENTS_FILENAME)
        if os.path.exists(events_file):
            os.remove(events_file)

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

        if previous_results:
            self._results += previous_results

        self.write_results(RESULTS_FILE)
        results_summary.delete_files()
        shutil.rmtree(self._results_dir)

        self._log_error_log_messages(self._output)

        bytes_consumed = get_directory_size_bytes(self._output, recursive=False)
        event = StructuredLogEvent(
            source="submitter",
            category=EVENT_CATEGORY_RESOURCE_UTIL,
            name=EVENT_NAME_BYTES_CONSUMED,
            message="main output directory size",
            bytes_consumed=bytes_consumed,
        )
        log_event(event)

        if reports:
            self.generate_reports(self._output)

        return result

    def write_results(self, filename):
        """Write the results to filename in the output directory."""
        data = OrderedDict()
        data["jade_version"] = jade.version.__version__
        now = datetime.datetime.now()
        data["timestamp"] = now.strftime("%m/%d/%Y %H:%M:%S")
        data["base_directory"] = os.getcwd()
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

    def _save_repository_info(self, registry):
        extensions = registry.list_extensions()
        extension_packages = set(["jade"])
        for ext in extensions:
            exec_module = ext[ExtensionClassType.EXECUTION].__module__
            name = exec_module.split(".")[0]
            extension_packages.add(name)

        for name in extension_packages:
            try:
                package = importlib.import_module(name)
                repo_info = RepositoryInfo(package)
                patch = os.path.join(self._output, f"{name}-diff.patch")
                repo_info.write_diff_patch(patch)
                logger.info("%s repository information: %s",
                            name, repo_info.summary())
            except InvalidParameter:
                pass

    @staticmethod
    def _log_error_log_messages(directory):
        for event in JobSubmitter.find_error_log_messages(directory):
            log_event(event)

    @staticmethod
    def find_error_log_messages(directory):
        """Parse output log files for error messages

        Parameters
        ----------
        directory : str
            output directory

        """
        errors = []
        substrings = (
            "DUE TO TIME LIMIT",  # includes slurmstepd, but check this first
            "srun",
            "slurmstepd",
            "Traceback",
        )

        filenames = [
            os.path.join(directory, x) for x in os.listdir(directory)
            if x.endswith(".e")
        ]

        if not filenames:
            return

        for line in fileinput.input(filenames):
            for substring in substrings:
                if substring in line:
                    event = StructuredLogEvent(
                        source="submitter",
                        category=EVENT_CATEGORY_ERROR,
                        name=EVENT_NAME_ERROR_LOG,
                        message="Detected error message in log.",
                        error=substring,
                        filename = fileinput.filename(),
                        line_number = fileinput.lineno(),
                        text = line.strip(),
                    )
                    yield event
                    # Only find one match in a single line.
                    break

    @staticmethod
    def generate_reports(directory):
        """Create reports summarizing the output results of a set of jobs.

        Parameters
        ----------
        directory : str
            output directory

        """
        commands = (
            (f"jade show-results -o {directory}", "results.txt"),
            (f"jade show-events -o {directory} --categories Error", "errors.txt"),
            (f"jade stats show -o {directory}", "stats.txt"),
        )

        reports = []
        for cmd in commands:
            output = {}
            ret = run_command(cmd[0], output=output)
            if ret != 0:
                return ret

            filename = os.path.join(directory, cmd[1])
            with open(filename, "w") as f_out:
                f_out.write(cmd[0] + "\n\n")
                f_out.write(output["stdout"])
                reports.append(filename)

        logger.info("Generated reports %s.", " ".join(reports))
        return 0

    def _submit_to_hpc(self, name, max_nodes, per_node_batch_size, verbose,
                       poll_interval, num_processes):
        queue_depth = max_nodes
        hpc_submitter = HpcSubmitter(
            name,
            self._config,
            self._config_file,
            self._hpc_config_file,
            self._results_dir,
        )

        hpc_submitter.run(
            self._output,
            queue_depth,
            per_node_batch_size,
            num_processes,
            poll_interval=poll_interval,
            verbose=verbose,
        )

        logger.info("All submitters have completed.")
