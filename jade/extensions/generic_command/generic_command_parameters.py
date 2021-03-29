"""Implements the JobParametersInterface for generic_command."""

from collections import namedtuple
from jade.jobs.job_parameters_interface import JobParametersInterface


class GenericCommandParameters(JobParametersInterface):
    """A class used for creating a job for a generic command."""

    parameters_type = namedtuple("GenericCommand", "command")
    _EXTENSION = "generic_command"

    def __init__(self, command, job_id=None, blocked_by=None, append_output_dir=False, ext=None,
                 estimated_run_minutes=None):
        self.command = command
        self.job_id = job_id  # Gets set when job is added to config.
                              # Uniquely identifies the job.
        self._estimated_run_minutes = estimated_run_minutes
        self.ext = ext or {}  # user-defined data
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
    def estimated_run_minutes(self):
        return self._estimated_run_minutes

    @estimated_run_minutes.setter
    def estimated_run_minutes(self, val):
        self._estimated_run_minutes = val

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
        # If job sizes get huge then we should exclude parameters with default values.
        return {
            "command": self.command,
            "job_id": self.job_id,
            "blocked_by": list(self.blocked_by),
            "extension": self.extension,
            "append_output_dir": self.append_output_dir,
            "estimated_run_minutes": self.estimated_run_minutes,
            "ext": self.ext,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            data["command"],
            job_id=data["job_id"],
            blocked_by={str(x) for x in data.get("blocked_by", [])},
            append_output_dir=data.get("append_output_dir", False),
            estimated_run_minutes=data.get("estimated_run_minutes"),
            ext=data.get("ext", {}),
        )

    def get_blocking_jobs(self):
        return self.blocked_by

    def remove_blocking_job(self, name):
        self.blocked_by.remove(name)

    def set_blocking_jobs(self, blocking_jobs):
        self.blocked_by = blocking_jobs
