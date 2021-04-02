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
        job : JobParametersInterface

        """
