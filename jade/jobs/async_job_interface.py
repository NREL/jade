"""Defines interface for async jobs."""

import abc


class AsyncJobInterface(abc.ABC):
    """Defines interface for async jobs."""

    @abc.abstractmethod
    def cancel(self):
        """Cancel the job."""

    @property
    @abc.abstractmethod
    def cancel_on_blocking_job_failure(self):
        """Cancel the job if a blocking job fails."""

    @abc.abstractmethod
    def is_complete(self):
        """Return True if the job is complete.

        Note: this must be called until the job actually completes.

        Returns
        -------
        bool

        """

    @abc.abstractproperty
    def name(self):
        """Return the job name.

        Returns
        -------
        str

        """

    @property
    @abc.abstractmethod
    def return_code(self):
        """Returns the return code of the job.

        Returns
        -------
        int
            0 means success, otherwise failure

        """

    @abc.abstractmethod
    def run(self):
        """Run the job."""

    @abc.abstractmethod
    def get_blocking_jobs(self):
        """Return the job names blocking this job.

        Returns
        -------
        set
            Empty set means that the job is not blocked.

        """

    @abc.abstractmethod
    def remove_blocking_job(self, name):
        """Remove the name from the job's blocking list.

        Parameters
        ----------
        name : str
            name of job that is now finished

        """

    @abc.abstractmethod
    def set_blocking_jobs(self, jobs):
        """Set the blocking jobs.

        Parameters
        ----------
        jobs : set
            names that are blocking

        """
