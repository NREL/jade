"""Defines base class for job execution analysis jobs."""

import os

from jade.common import ANALYSIS_DIR
from jade.jobs.job_execution_interface import JobExecutionInterface


class AnalysisExecutionBase(JobExecutionInterface):
    """Base class for analysis jobs. This job type is intended to run on the
    output results of another job."""

    def __init__(self, output_dir, simulations_dir):
        self._analysis_dir = self.get_analysis_dir(output_dir)
        self._simulations_dir = simulations_dir
        os.makedirs(self._analysis_dir, exist_ok=True)

    @staticmethod
    def get_analysis_dir(output_dir):
        """Get the analysis directory to use.

        Parameters
        ----------
        output_dir : str

        Returns
        -------
        str

        """
        return os.path.join(output_dir, ANALYSIS_DIR)

    @property
    def results_directory(self):
        """Return the results directory created by the simulation."""
        return self._analysis_dir

    def list_results_files(self):
        """Return a list of result filenames created by the simulation."""
        return [os.path.join(self._analysis_dir, x) for x in os.listdir(self._analysis_dir)]

    def post_process(self, **kwargs):
        pass
