"""Performs postprocessing and analysis on job results."""

import logging
import os

from jade.common import JOBS_OUTPUT_DIR
from jade.enums import JobCompletionStatus
from jade.exceptions import ExecutionError, InvalidParameter
from jade.result import ResultsSummary
from jade.utils.utils import load_data


logger = logging.getLogger(__name__)


class JobAnalysis:
    """Provides functionality to analyze job results."""

    def __init__(self, output_dir, config):
        self._output_dir = output_dir
        self._config = config
        self._results = ResultsSummary(output_dir)

    def get_job(self, job_name):
        """Return the job from the config file with job_name.

        Parameters
        ----------
        job_name : str

        Returns
        -------
        namedtuple

        """
        return self._config.get_job(job_name)

    def get_successful_result(self, job_name):
        """Return the job result from the results file.
        Refer to :func:`~jade.result.ResultSummary.get_successful_result`.

        """
        return self._results.get_successful_result(job_name)

    def get_simulation(self, job_name):
        """Return a simulation object for the job_name.

        Parameters
        ----------
        job_name : str

        Returns
        -------
        JobExecutionInterface

        """
        # Make sure it was successful, otherwise it will raise.
        self.get_successful_result(job_name)

        job = self.get_job(job_name)
        simulation = self._config.create_from_result(
            job, os.path.join(self._output_dir, JOBS_OUTPUT_DIR)
        )
        return simulation

    def list_results(self):
        """Return a list of Result objects."""
        return self._results.list_results()

    @property
    def output_dir(self):
        return self._output_dir

    def show_results(self, only_failed=False, only_successful=False):
        """Show the results in terminal."""
        return self._results.show_results(only_failed=only_failed, only_successful=only_successful)


def get_result(results_file, job_name):
    """Return the job result from the results file.

    Parameters
    ----------
    results_file : str
    job_name : str

    Returns
    -------
    dict

    Raises
    ------
    InvalidParameter
        Raised if job_name is not found.

    """
    results = load_data(results_file)
    for result in results:
        if result.name == job_name:
            return result

    raise InvalidParameter(f"result not found {job_name}")


def get_successful_result(results_file, job_name):
    """Return the job result from the results file.

    Parameters
    ----------
    results_file : str
    job_name : str

    Returns
    -------
    dict

    Raises
    ------
    InvalidParameter
        Raised if job_name is not found.
    ExecutionError
        Raised if the result was not successful.

    """
    result = get_result(results_file, job_name)
    if not result.is_successful():
        raise ExecutionError(f"result was not successful: {result}")

    return result
