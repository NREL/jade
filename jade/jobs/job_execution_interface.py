"""Defines interface for simulations."""


import abc
import logging


logger = logging.getLogger(__name__)


class JobExecutionInterface(abc.ABC):
    """Interface definition for simulations."""

    @staticmethod
    def collect_results(output_dir):
        """Collect result data from output directory.

        Parameters
        ----------
        output_dir : str

        Returns
        -------
        list of dict

        """
        logger.debug("Collect results from %s", output_dir)
        # subclasses can override.
        return []

    @classmethod
    @abc.abstractmethod
    def create(cls, job_inputs, job, output):
        """Creates an instance of a JobExecutionInterface."""

    @staticmethod
    @abc.abstractmethod
    def generate_command(job, output, config_file, verbose=False):
        """Generate a command for a job to be run in a subprocess.

        Parameters
        ----------
        job : class.parameters_type
        output : str
            output directory
        config_file : str
            job configuration file

        Returns
        -------
        str
            command that can be executed in the OS

        """

    @property
    @abc.abstractmethod
    def results_directory(self):
        """Return the results directory created by the simulation."""

    @abc.abstractmethod
    def list_results_files(self):
        """Return a list of result filenames created by the simulation."""

    @abc.abstractmethod
    def post_process(self, **kwargs):
        """Run post-process operations on data."""

    @abc.abstractmethod
    def run(self):
        """Runs the simulation."""
