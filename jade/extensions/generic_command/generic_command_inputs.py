"""The job configuration class for collecting all jobs."""

import os
from jade.jobs.job_inputs_interface import JobInputsInterface
from jade.extensions.generic_command import GenericCommandParameters


class GenericCommandInputs(JobInputsInterface):
    """A class used for configuring generic_command jobs."""

    def __init__(self, filename):
        """
        Parameters
        ----------
        filename : str
            Input file containing commands, one line per command

        """
        self._filename = filename
        self._parameters = []
        with open(filename) as f_in:
            self._parameters = []
            for line in f_in.readlines():
                line = line.strip()
                if line:
                    self._parameters.append(GenericCommandParameters(command=line))

    @property
    def base_directory(self):
        return os.path.dirname(self._filename)

    def get_available_parameters(self):
        return self._parameters

    def iter_jobs(self):
        """Return a list of auto-regression jobs"""
        for param in self._parameters:
            yield param
