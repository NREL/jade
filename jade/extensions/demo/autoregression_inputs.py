"""
The job configuration class for collecting all jobs.
"""
import os
from jade.jobs.job_inputs_interface import JobInputsInterface
from jade.utils.utils import load_data
from jade.extensions.demo.autoregression_parameters import AutoRegressionParameters


class AutoRegressionInputs(JobInputsInterface):
    """
    A class used for configuring auto-regression analysis jobs.
    """

    INPUTS_FILE = "inputs.json"

    def __init__(self, base_directory):
        self._base_directory = base_directory
        self._parameters = {}
        self.get_available_parameters()

    @property
    def base_directory(self):
        """The directory contains inputs data"""
        return self._base_directory

    def get_available_parameters(self):
        """Collect all available auto-regression jobs"""
        inputs_file = os.path.join(self._base_directory, self.INPUTS_FILE)
        inputs = load_data(inputs_file)

        for param in inputs:
            job = AutoRegressionParameters(
                country=param["country"], data=os.path.join(self._base_directory, param["data"])
            )

            self._parameters[job.name] = job

    def iter_jobs(self):
        """Return a list of auto-regression jobs"""
        return list(self._parameters.values())
