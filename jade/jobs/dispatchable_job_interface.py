"""Defines interface for dispatchable jobs.."""

import abc

from jade.jobs.async_job_interface import AsyncJobInterface


class DispatchableJobInterface(AsyncJobInterface, abc.ABC):
    """Defines interface for dispatchable jobs."""

    @property
    def job(self):
        """Get the job.

        Parameters
        ----------
        job : namedtuple

        """

    @abc.abstractmethod
    def set_results_filename_suffix(self, suffix):
        """Set this suffix when the results get written."""
        assert False
