"""Implements the JobParametersInterface for generic_command."""

from collections import namedtuple
from jade.jobs.job_parameters_interface import JobParametersInterface


class GenericCommandParameters(JobParametersInterface):
    """A class used for creating a job for a generic command."""

    parameters_type = namedtuple("GenericCommand", "command")

    def __init__(self, command, job_id=None):
        self.command = command
        self.job_id = job_id  # Gets set when job is added to config.
                              # Uniquely identifies the job.

    def __str__(self):
        return "<GenericCommandParameters: {}>".format(self.name)

    @property
    def name(self):
        return self._create_name()

    def _create_name(self):
        return str(self.job_id)

    def serialize(self):
        assert self.job_id is not None
        return {
            "command": self.command,
            "job_id": self.job_id,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(data["command"], job_id=data["job_id"])
