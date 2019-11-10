"""Defines interface for async jobs."""

import abc


class AsyncJobInterface(abc.ABC):
    """Defines interface for async jobs."""

    @abc.abstractmethod
    def is_complete(self):
        """Return True if the job is complete.

        Note: this must be called until the job actually completes.

        Returns
        -------
        bool

        """

    @abc.abstractmethod
    def name(self):
        """Return the job name.

        Returns
        -------
        str

        """

    @abc.abstractmethod
    def run(self):
        """Run the job."""
