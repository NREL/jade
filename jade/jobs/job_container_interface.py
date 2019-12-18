"""Defines interface for job containers."""

import abc


class JobContainerInterface:
    """Defines interface for job containers."""

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
    def get_jobs(self, sort_by_key=False):
        """Return all jobs.

        Parameters
        ----------
        sort_by_key : bool
            If true, sort the list by key.

        Returns
        -------
        list

        """

    @abc.abstractmethod
    def get_num_jobs(self, sort_by_key=False):
        """Return the number of jobs.

        Returns
        -------
        int

        """

    @abc.abstractmethod
    def iter_jobs(self):
        """Yields a generator over all jobs.

        Yields
        ------
        iterator over JobParametersInterface

        """

    def list_jobs(self):
        """Return a list of all jobs.

        Returns
        ------
        list
            list of JobParametersInterface

        """
        return list(self.iter_jobs)

    @abc.abstractmethod
    def remove_job(self, job):
        """Remove a job from the configuration.

        Parameters
        ----------
        job : JobParametersInterface

        """
