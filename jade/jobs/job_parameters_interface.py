"""Defines interface for all job parameters."""

import abc


class JobParametersInterface(abc.ABC):
    """Job Parameters interface definitio."""

    DELIMITER = "__"

    @property
    @abc.abstractmethod
    def estimated_run_minutes(self):
        """Return the estimated execution time or None if it isn't known.

        Returns
        -------
        float | None

        """

    @property
    @abc.abstractmethod
    def extension(self):
        """Return the extension name.

        Returns
        -------
        str

        """

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
    def set_blocking_jobs(self, blocking_jobs):
        """Set the blocking jobs.

        Parameters
        ----------
        blocking_jobs : set

        """

    @property
    @abc.abstractmethod
    def cancel_on_blocking_job_failure(self):
        """Return False if the job should be canceled if any blocking job fails.

        Returns
        -------
        bool

        """

    @property
    @abc.abstractmethod
    def submission_group(self):
        """Return the submission group for the job."""

    @submission_group.setter
    @abc.abstractmethod
    def submission_group(self, group):
        """Set the submission group for the job.

        Parameters
        ----------
        group : SubmissionGroup

        """
