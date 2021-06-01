"""Contains common functionality for managing jobs."""

import abc
import json
import logging
import os

from jade.common import JOBS_OUTPUT_DIR
from jade.jobs.job_configuration_factory import create_config_from_file


logger = logging.getLogger(__name__)


class JobManagerBase(abc.ABC):
    """Base class for managing jobs."""

    def __init__(self, config_file, output_dir):
        self._config = create_config_from_file(config_file)
        self._config_file = config_file
        self._output = output_dir
        self._jobs_output = os.path.join(self._output, JOBS_OUTPUT_DIR)
        self._results = []  # contains Result objects

        os.makedirs(self._output, exist_ok=True)
        os.makedirs(self._jobs_output, exist_ok=True)

    @property
    def config(self):
        return self._config

    def get_num_jobs(self):
        """Get the number of jobs to execute.

        Returns
        -------
        int

        """
        return self._config.get_num_jobs()

    def get_completed_results(self):
        """Get results of completed jobs.

        Returns
        -------
        list of Result

        """
        return self._results[:]

    def get_results_summmary(self):
        """Get a job results.

        Returns
        -------
        dict

        """
        num_successful = 0
        num_failed = 0
        for job in self._results:
            if job.status == 0:
                num_successful += 1
            else:
                num_failed += 1

        results = {
            "num_jobs": self.get_num_jobs(),
            "num_complete": len(self._results),
            "num_successful": num_successful,
            "num_failed": num_failed,
        }

        return results

    def get_results_summmary_report(self):
        """Get a summary of job results in text form.

        Returns
        -------
        str

        """
        data = self.get_results_summmary()
        return json.dumps(data, indent=4)

    def get_batch_post_process_config(self):
        """Get the batch post-process config data"""
        return self._config.batch_post_process_config

    def _handle_submission_groups_after_deserialize(self, cluster):
        # The JobConfiguration will not have any groups if the user didn't define any.
        # Reload from the cluster config.
        if not self._config.submission_groups:
            assert len(cluster.config.submission_groups) == 1
            self._config.append_submission_group(cluster.config.submission_groups[0])
