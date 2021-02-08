"""Implements the JobParametersInterface for generic_command."""

from collections import namedtuple
from jade.jobs.job_parameters_interface import JobParametersInterface


class GenericCommandParameters(JobParametersInterface):
    """A class used for creating a job for a generic command."""

    parameters_type = namedtuple("GenericCommand", "command")
    _EXTENSION = "generic_command"

    def __init__(self, command, job_id=None, blocked_by=None, append_output_dir=False):
        self.command = command
        self.job_id = job_id  # Gets set when job is added to config.
                              # Uniquely identifies the job.
        self.blocked_by = set()
        if blocked_by is not None:
            for job_id in blocked_by:
                self.blocked_by.add(str(job_id))

        # Indicates whether the output directory should be appended to the
        # command at runtime.
        self.append_output_dir = append_output_dir

    def __str__(self):
        return "<GenericCommandParameters: {}>".format(self.name)

    @property
    def extension(self):
        return self._EXTENSION

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
            "blocked_by": list(self.blocked_by),
            "extension": self.extension,
            "append_output_dir": self.append_output_dir,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            data["command"],
            job_id=data["job_id"],
            blocked_by={str(x) for x in data["blocked_by"]},
            append_output_dir=data["append_output_dir"],
        )

    def get_blocking_jobs(self):
        return self.blocked_by

    def remove_blocking_job(self, name):
        self.blocked_by.remove(name)

    def set_blocking_jobs(self, blocking_jobs):
        self.blocked_by = blocking_jobs
