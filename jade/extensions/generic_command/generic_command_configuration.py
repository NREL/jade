"""Implement JobConfiguration for generic_command."""

from jade.exceptions import InvalidConfiguration
from jade.jobs.job_configuration import JobConfiguration
from jade.extensions.generic_command.generic_command_inputs import GenericCommandInputs


class GenericCommandConfiguration(JobConfiguration):
    """A class used to configure generic_command jobs."""

    def __init__(self, **kwargs):
        """
        Init GenericCommand class

        Parameters
        ----------
        kwargs, extra arguments

        """
        self._cur_job_id = 1
        super(GenericCommandConfiguration, self).__init__(**kwargs)

    @classmethod
    def auto_config(
        cls, inputs, cancel_on_blocking_job_failure=False, minutes_per_job=None, **kwargs
    ):
        """Create a configuration from all available inputs."""
        if isinstance(inputs, str):
            inputs = GenericCommandInputs(inputs)

        config = GenericCommandConfiguration(**kwargs)
        for job_param in inputs.iter_jobs():
            job_param.cancel_on_blocking_job_failure = cancel_on_blocking_job_failure
            job_param.estimated_run_minutes = minutes_per_job
            config.add_job(job_param)

        return config

    def _serialize(self, data):
        pass

    def add_job(self, job):
        # Overrides JobConfiguration.add_job so that it can add a unique
        # identifier to each job.

        # This will not be true at deserialization.
        if job.job_id is None:
            job.job_id = self._cur_job_id
            self._cur_job_id += 1

        if not job.command:
            raise InvalidConfiguration(f"command cannot be emtpy: job_id={job.job_id}")
        self._jobs.add_job(job)

    def create_from_result(self, job, output_dir):
        return None
