"""Provides ability to run jobs locally or on HPC."""

from collections import OrderedDict
import datetime
import fileinput
import importlib
import logging
import os
import shutil
import time

import jade
from jade.common import (
    CONFIG_FILE, JOBS_OUTPUT_DIR, OUTPUT_DIR, RESULTS_FILE, HPC_CONFIG_FILE,
    get_results_filename
)
from jade.enums import Status
from jade.events import (
    EVENTS_FILENAME, EVENT_NAME_ERROR_LOG,
    StructuredLogEvent, EVENT_CATEGORY_ERROR, EVENT_CATEGORY_RESOURCE_UTIL,
    EVENT_NAME_BYTES_CONSUMED, EVENT_NAME_SUBMIT_STARTED,
    EVENT_NAME_SUBMIT_COMPLETED
)
from jade.exceptions import InvalidParameter
from jade.extensions.registry import Registry, ExtensionClassType
from jade.hpc.common import HpcType
from jade.hpc.hpc_manager import HpcManager
from jade.hpc.hpc_submitter import HpcSubmitter
from jade.jobs.cluster import Cluster
from jade.jobs.job_configuration_factory import create_config_from_previous_run
from jade.jobs.job_manager_base import JobManagerBase
from jade.jobs.job_runner import JobRunner
from jade.jobs.results_aggregator import ResultsAggregator
from jade.models import LocalHpcConfig
from jade.loggers import log_event
from jade.result import serialize_results, ResultsSummary
from jade.utils.repository_info import RepositoryInfo
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import dump_data, get_directory_size_bytes
import jade.version


logger = logging.getLogger(__name__)


class JobSubmitter(JobManagerBase):
    """Submits jobs for execution locally or on an HPC."""
    def __init__(self, config_file, output, is_new):
        """Internal constructor. Callers should use create() or reload()."""
        super(JobSubmitter, self).__init__(config_file, output)
        self._hpc = None
        self._config_file = config_file
        self._is_new = is_new

    @classmethod
    def create(cls, config_file, output=OUTPUT_DIR):
        """Creates a new instance.

        Parameters
        ----------
        config_file : JobConfiguration
            configuration for simulation
        output : str
            Output directory

        """
        master_file = os.path.join(output, CONFIG_FILE)
        shutil.copyfile(config_file, master_file)
        return cls(master_file, output, True)

    @classmethod
    def reload(cls, output):
        """Reloads an instance from an existing directory."""
        return cls(os.path.join(output, CONFIG_FILE), output, False)

    def __repr__(self):
        return f"""hpc_config_file={self._hpc_config_file}
num_jobs={self.get_num_jobs()}
results_summary={self.get_results_summmary_report()}"""

    def cancel_jobs(self):
        """Cancel running and pending jobs."""
        # TODO

    def submit_jobs(self, cluster, force_local=False, previous_results=None):
        """Submit simulations. Auto-detect whether the current system is an HPC
        and submit to its queue. Otherwise, run locally.

        Parameters
        ----------
        force_local : bool
            If on HPC, run jobs through subprocess as if local.

        Returns
        -------
        Status

        """
        if self._is_new:
            logger.info("Submit %s jobs for execution.",
                        self._config.get_num_jobs())
            logger.info("JADE version %s", jade.version.__version__)
            registry = Registry()
            loggers = registry.list_loggers()
            logger.info("Registered modules for logging: %s", ", ".join(loggers))
            self._save_repository_info(registry)
            self._config.check_job_dependencies()

            results_aggregator = ResultsAggregator(get_results_filename(self._output))
            results_aggregator.create_file()

            # If an events summary file exists, it is invalid.
            events_file = os.path.join(self._output, EVENTS_FILENAME)
            if os.path.exists(events_file):
                os.remove(events_file)

            event = StructuredLogEvent(
                source="submitter",
                category=EVENT_CATEGORY_RESOURCE_UTIL,
                name=EVENT_NAME_SUBMIT_COMPLETED,
                message="job submission started",
                num_jobs=self.get_num_jobs(),
            )
            log_event(event)

        result = Status.IN_PROGRESS
        self._hpc = HpcManager(cluster.config.submitter_params.hpc_config, self._output)

        if self._hpc.hpc_type == HpcType.LOCAL or force_local:
            runner = JobRunner(self._config_file, output=self._output)
            num_processes = cluster.config.submitter_params.num_processes
            verbose = cluster.config.submitter_params.verbose
            result = runner.run_jobs(verbose=verbose, num_processes=num_processes)
            is_complete = True
        else:
            is_complete = self._submit_to_hpc(cluster)

        if is_complete:
            result = self._handle_completion(cluster, previous_results)

        return result

    def _handle_completion(self, cluster, previous_results):
        result = Status.GOOD
        self._results = ResultsAggregator.list_results(self._output)
        if len(self._results) != self._config.get_num_jobs():
            logger.error("Number of results doesn't match number of jobs: "
                         "results=%s jobs=%s. Check for process crashes "
                         "or HPC timeouts.",
                         len(self._results), self._config.get_num_jobs())
            result = Status.ERROR

        if previous_results:
            self._results += previous_results

        self.write_results_summary(RESULTS_FILE)
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

        event = StructuredLogEvent(
            source="submitter",
            category=EVENT_CATEGORY_RESOURCE_UTIL,
            name=EVENT_NAME_SUBMIT_COMPLETED,
            message="job submission completed",
            num_jobs=self.get_num_jobs(),
        )
        log_event(event)

        if cluster.config.submitter_params.generate_reports:
            self.generate_reports(self._output)

        cluster.mark_complete()

        if cluster.config.pipeline_stage_index is not None:
            # The pipeline directory must be the one above this one.
            pipeline_dir = os.path.dirname(self._output)
            next_stage = cluster.config.pipeline_stage_index + 1
            cmd = f"jade pipeline try-submit {pipeline_dir} " \
                f"--stage-index={next_stage} " \
                f"--return-code={result.value}"
            run_command(cmd) 

        return result

    def write_results_summary(self, filename):
        """Write the results to filename in the output directory."""
        data = OrderedDict()
        data["jade_version"] = jade.version.__version__
        now = datetime.datetime.now()
        data["timestamp"] = now.strftime("%m/%d/%Y %H:%M:%S")
        data["base_directory"] = os.getcwd()
        results = self._build_results()
        data["results_summary"] = results["summary"]
        data["results"] = results["results"]

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

    def _submit_to_hpc(self, cluster):
        hpc_submitter = HpcSubmitter(
            self._config,
            self._config_file,
            self._results_dir,
            cluster,
            self._output,
        )

        if hpc_submitter.run():
            logger.info("All submitters have completed.")
            return True

        logger.debug("jobs are still pending")
        return False

    @staticmethod
    def run_submit_jobs(
        config_file,
        output,
        params,
        restart_failed=False,
        restart_missing=False,
        pipeline_stage_index=None
    ):
        """Allows submission from an existing Python process."""
        os.makedirs(output, exist_ok=True)
        mgr = JobSubmitter.create(config_file, output=output)
        cluster = Cluster.create(
            output,
            params,
            mgr.config,
            pipeline_stage_index=pipeline_stage_index,
        )

        previous_results = []
        if restart_failed:
            failed_job_config = create_config_from_previous_run(
                config_file,
                output,
                result_type='failed',
            )
            previous_results = ResultsSummary(output).get_successful_results()
            config_file = "failed_job_inputs.json"
            failed_job_config.dump(config_file)

        if restart_missing:
            missing_job_config = create_config_from_previous_run(
                config_file,
                output,
                result_type='missing',
            )
            config_file = "missing_job_inputs.json"
            missing_job_config.dump(config_file)
            previous_results = ResultsSummary(output).list_results()

        force_local = isinstance(params.hpc_config, LocalHpcConfig)
        ret = 1
        try:
            status = mgr.submit_jobs(
                cluster,
                force_local=force_local,
                previous_results=previous_results,
            )
            if status == Status.IN_PROGRESS:
                check_cmd = f"jade show-status -o {output}"
                print(f"Jobs are in progress. Run '{check_cmd}' for updates.")
                ret = 0
            else:
                ret = status.value
        finally:
            cluster.demote_from_submitter()

        return ret
