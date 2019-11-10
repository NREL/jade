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
