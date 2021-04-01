"""Defines interface for job containers."""

import abc


class JobContainerInterface(abc.ABC):
    """Defines interface for job containers."""

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass

    @abc.abstractmethod
    def add_job(self, job):
        """Add a job to the configuration.

        Parameters
        ----------
        job : JobParametersInterface

        """

    @abc.abstractmethod
    def clear(self):
        """Clear all configured jobs."""

    @abc.abstractmethod
    def get_job(self, name):
        """Return the job matching name.

        Returns
        -------
        namedtuple

        """

    @abc.abstractmethod
    def get_jobs(self, sort=False):
        """Return all jobs.

        Parameters
        ----------
        sort : bool

        Returns
        -------
        list

        """

    def list_jobs(self):
        """Return a list of all jobs.

        Returns
        ------
        list
            list of JobParametersInterface

        """
        return list(iter(self))

    @abc.abstractmethod
    def remove_job(self, job):
        """Remove a job from the configuration.

        Parameters
        ----------
        job : JobParametersInterface

        """
