"""Defines interface for all job parameters."""

import abc


class JobParametersInterface:
    """Job Parameters interface definitio."""

    DELIMITER = "__"

    @property
    @abc.abstractmethod
    def name(self):
        """Return the job name. The job name must be unique in a configuration
        and must be usable as a directory or file name on any filesystem.

        Returns
        -------
        str

        """

    @abc.abstractmethod
    def serialize(self):
        """Serialize data to a dictionary.

        Returns
        -------
        dict

        """

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, data):
        """Deserialize parameters from a dictionary.

        Parameters
        ----------
        data : dict

        Returns
        -------
        JobParametersInterface

        """

    # Derived classes must override the next two methods if jobs have ordering
    # requirements.
    # Default behavior is provided for jobs that have no dependencies.
    def get_blocking_jobs(self):
        """Return the job names blocking this job.

        Returns
        -------
        set
            Empty set means that the job is not blocked.

        """
        return set()

    def remove_blocking_job(self, name):
        """Remove the name from the job's blocking list.

        Parameters
        ----------
        name : str
            name of job that is now finished

        """
        assert False, f"method not implemented for {self.__class__.__name__}"
