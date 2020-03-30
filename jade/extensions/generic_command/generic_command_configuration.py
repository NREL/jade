"""Implement JobConfiguration for generic_command."""

from jade.jobs.job_container_by_key import JobContainerByKey
from jade.jobs.job_configuration import JobConfiguration
from jade.extensions.generic_command.generic_command_inputs import \
    GenericCommandInputs
from jade.extensions.generic_command.generic_command_parameters import \
    GenericCommandParameters


class GenericCommandConfiguration(JobConfiguration):
    """A class used to configure generic_command jobs."""

    def __init__(self, job_inputs, **kwargs):
        """
        Init GenericCommand class

        Parameters
        ----------
        job_inputs: :obj:`GenericCommandInputs`
            The instance of :obj:`GenericCommandInputs`
        kwargs, extra arguments
        """
        self._cur_job_id = 1
        super(GenericCommandConfiguration, self).__init__(
            inputs=job_inputs,
            container=JobContainerByKey(),
            job_parameters_class=GenericCommandParameters,
            extension_name="generic_command",
            **kwargs
        )

    @classmethod
    def auto_config(cls, inputs, **kwargs):
        """Create a configuration from all available inputs."""
        if isinstance(inputs, str):
            job_inputs = GenericCommandInputs(inputs)
        else:
            job_inputs = inputs

        config = GenericCommandConfiguration(job_inputs, **kwargs)
        for job_param in config.inputs.iter_jobs():
            config.add_job(job_param)

        return config

    def _serialize(self, data):
        """Fill in instance-specific information."""

    def add_job(self, job):
        # Overrides JobConfiguration.add_job so that it can add a unique
        # identifier to each job.

        # This will not be true at deserialization.
        if job.job_id is None:
            job.job_id = self._cur_job_id
            self._cur_job_id += 1

        self._jobs.add_job(job)

    def create_from_result(self, job, output_dir):
        return None

    def get_job_inputs(self):
        return self._inputs
