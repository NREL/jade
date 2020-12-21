"""Defines interface for Configuration Inputs."""

import abc


class JobInputsInterface(abc.ABC):
    """Interface definition for configuration inputs."""

    @property
    def base_directory(self):
        """Return the base directory."""

    @abc.abstractmethod
    def get_available_parameters(self):
        """Return a dictionary containing all available parameters."""
