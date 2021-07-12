"""Provides ability to run jobs locally or on HPC."""

from collections import OrderedDict
import datetime
import fileinput
import importlib
import logging
import os
import shutil

import jade
from jade.common import (
    CONFIG_FILE,
    JOBS_OUTPUT_DIR,
    OUTPUT_DIR,
    RESULTS_FILE,
    HPC_CONFIG_FILE,
)
from jade.enums import JobCompletionStatus, Status
from jade.events import (
    EVENTS_FILENAME,
    EVENT_NAME_ERROR_LOG,
    StructuredLogEvent,
    EVENT_CATEGORY_ERROR,
    EVENT_CATEGORY_RESOURCE_UTIL,
    EVENT_NAME_BYTES_CONSUMED,
    EVENT_NAME_SUBMIT_STARTED,
    EVENT_NAME_SUBMIT_COMPLETED,
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
from jade.models import SubmitterParams
from jade.models.submission_group import make_submission_group_lookup
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
        """Internal constructor. Callers should use create() or load()."""
        super().__init__(config_file, output)
        self._hpc = None
        self._config_file = config_file
        self._is_new = is_new

    @classmethod
    def create(cls, config_file, params: SubmitterParams, output=OUTPUT_DIR):
        """Creates a new instance.

        Parameters
        ----------
        config_file : JobConfiguration
            configuration for simulation
        params: SubmitterParams
        output : str
            Output directory

        """
        master_file = os.path.join(output, CONFIG_FILE)
        shutil.copyfile(config_file, master_file)
        mgr = cls(master_file, output, True)
        mgr.run_checks(params)
        return mgr

    @classmethod
    def load(cls, output):
        """Loads an instance from an existing directory."""
        return cls(os.path.join(output, CONFIG_FILE), output, False)

    def __repr__(self):
        return f"""num_jobs={self.get_num_jobs()}
results_summary={self.get_results_summmary_report()}"""

    def cancel_jobs(self, cluster):
        """Cancel running and pending jobs."""
        groups = make_submission_group_lookup(cluster.config.submission_groups)
        hpc = HpcManager(groups, self._output)
        for job_id in cluster.job_status.hpc_job_ids:
            hpc.cancel_job(job_id)

        cluster.mark_complete(canceled=True)

    def submit_jobs(self, cluster, force_local=False):
        """Submit simulations. Auto-detect whether the current system is an HPC
        and submit to its queue. Otherwise, run locally.

        Parameters
        ----------
        cluster : Cluster
        force_local : bool
            If on HPC, run jobs through subprocess as if local.

        Returns
        -------
        Status

        """
        if self._is_new:
            logger.info("Submit %s jobs for execution.", self._config.get_num_jobs())
            logger.info("JADE version %s", jade.version.__version__)
            registry = Registry()
            loggers = registry.list_loggers()
            logger.info("Registered modules for logging: %s", ", ".join(loggers))
            self._save_repository_info(registry)

            ResultsAggregator.create(self._output)

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
        else:
            self._handle_submission_groups_after_deserialize(cluster)

        result = Status.IN_PROGRESS
        group = self._config.get_default_submission_group()
        groups = make_submission_group_lookup(cluster.config.submission_groups)
        self._hpc = HpcManager(groups, self._output)

        if self._hpc.hpc_type == HpcType.LOCAL or force_local:
            runner = JobRunner(self._config_file, output=self._output)
            num_processes = group.submitter_params.num_processes
            verbose = group.submitter_params.verbose
            result = runner.run_jobs(verbose=verbose, num_processes=num_processes)
            agg = ResultsAggregator.load(self._output)
            agg.process_results()
            is_complete = True
        else:
            is_complete = self._submit_to_hpc(cluster)

        if is_complete:
            result = self._handle_completion(cluster)

        return result

    def _handle_completion(self, cluster):
        result = Status.GOOD
        self._results = ResultsAggregator.list_results(self._output)
        if len(self._results) != self._config.get_num_jobs():
            finished_jobs = {x.name for x in self._results}
            all_jobs = {x.name for x in self._config.iter_jobs()}
            missing_jobs = sorted(all_jobs.difference(finished_jobs))
            logger.error(
                "Error in result totals. num_results=%s total_num_jobs=%s",
                len(self._results),
                self._config.get_num_jobs(),
            )
            logger.error(
                "These jobs did not finish: %s. Check for process crashes or HPC timeouts.",
                missing_jobs,
            )
            result = Status.ERROR
        else:
            missing_jobs = []

        self.write_results_summary(RESULTS_FILE, missing_jobs)
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

        group = self._config.get_default_submission_group()
        if group.submitter_params.generate_reports:
            include_stats = bool(group.submitter_params.resource_monitor_interval)
            self.generate_reports(self._output, include_stats=include_stats)

        cluster.mark_complete()

        if cluster.config.pipeline_stage_num is not None:
            # The pipeline directory must be the one above this one.
            pipeline_dir = os.path.dirname(self._output)
            next_stage = cluster.config.pipeline_stage_num + 1
            cmd = (
                f"jade pipeline submit-next-stage {pipeline_dir} "
                f"--stage-num={next_stage} "
                f"--return-code={result.value}"
            )
            run_command(cmd)

        return result

    def write_results_summary(self, filename, missing_jobs):
        """Write the results to filename in the output directory."""
        data = OrderedDict()
        data["jade_version"] = jade.version.__version__
        now = datetime.datetime.now()
        data["timestamp"] = now.strftime("%m/%d/%Y %H:%M:%S")
        data["base_directory"] = os.getcwd()
        results = self._build_results(missing_jobs)
        data["results_summary"] = results["summary"]
        data["missing_jobs"] = missing_jobs
        data["results"] = results["results"]

        output_file = os.path.join(self._output, filename)
        dump_data(data, output_file)

        logger.info("Wrote results to %s.", output_file)
        num_successful = results["summary"]["num_successful"]
        num_canceled = results["summary"]["num_canceled"]
        num_failed = results["summary"]["num_failed"]
        num_missing = len(missing_jobs)
        total = num_successful + num_failed + num_missing
        log_func = logger.info if num_successful == total else logger.warning
        log_func(
            "Successful=%s Failed=%s Canceled=%s Missing=%s Total=%s",
            num_successful,
            num_failed,
            num_canceled,
            num_missing,
            total,
        )

        return output_file

    def _build_results(self, missing_jobs):
        num_successful = 0
        num_failed = 0
        num_canceled = 0
        for result in self._results:
            if result.is_successful():
                num_successful += 1
            elif result.is_failed():
                num_failed += 1
            else:
                assert result.is_canceled(), str(result)
                num_canceled += 1

        return {
            "results": serialize_results(self._results),
            "summary": {
                "num_successful": num_successful,
                "num_failed": num_failed,
                "num_canceled": num_canceled,
                "num_missing": len(missing_jobs),
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
                logger.info("%s repository information: %s", name, repo_info.summary())
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

        filenames = [os.path.join(directory, x) for x in os.listdir(directory) if x.endswith(".e")]

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
                        filename=fileinput.filename(),
                        line_number=fileinput.lineno(),
                        text=line.strip(),
                    )
                    yield event
                    # Only find one match in a single line.
                    break

    @staticmethod
    def generate_reports(directory, include_stats=False):
        """Create reports summarizing the output results of a set of jobs.

        Parameters
        ----------
        directory : str
            output directory

        """
        commands = [
            (f"jade show-results -o {directory}", "results.txt"),
            (f"jade show-events -o {directory} --categories Error", "errors.txt"),
        ]
        if include_stats:
            commands.append((f"jade stats plot -o {directory}", None))
            commands.append((f"jade stats show -o {directory}", "stats.txt"))
            commands.append((f"jade stats show -o {directory} -j", "stats_summary.json"))

        reports = []
        for cmd in commands:
            output = {}
            ret = run_command(cmd[0], output=output)
            if ret != 0:
                return ret

            if cmd[1] is not None:
                filename = os.path.join(directory, cmd[1])
                with open(filename, "w") as f_out:
                    if "json" not in cmd[1]:
                        f_out.write(cmd[0] + "\n\n")
                    f_out.write(output["stdout"])
                    reports.append(filename)

        logger.info("Generated reports %s.", " ".join(reports))

        return 0

    def _submit_to_hpc(self, cluster):
        hpc_submitter = HpcSubmitter(
            self._config,
            self._config_file,
            cluster,
            self._output,
        )

        if hpc_submitter.run():
            logger.info("All submitters have completed.")
            return True

        logger.debug("jobs are still pending")
        return False

    def run_checks(self, params: SubmitterParams):
        """Checks the configuration for errors. May mutate the config."""
        self._config.check_job_dependencies(params)
        self._config.check_submission_groups(params)

    @staticmethod
    def run_submit_jobs(config_file, output, params, pipeline_stage_num=None):
        """Allows submission from an existing Python process."""
        os.makedirs(output, exist_ok=True)

        mgr = JobSubmitter.create(config_file, params, output=output)
        cluster = Cluster.create(
            output,
            mgr.config,
            pipeline_stage_num=pipeline_stage_num,
        )

        local = params.hpc_config.hpc_type == HpcType.LOCAL
        ret = 1
        try:
            status = mgr.submit_jobs(cluster, force_local=local)
            if status == Status.IN_PROGRESS:
                check_cmd = f"jade show-status -o {output}"
                if not params.dry_run:
                    print(f"Jobs are in progress. Run '{check_cmd}' for updates.")
                ret = 0
            else:
                ret = status.value
        finally:
            cluster.demote_from_submitter()
            if local:
                # These files were not used in this case.
                cluster.delete_files_internal()

        return ret
