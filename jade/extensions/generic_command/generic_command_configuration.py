"""Implement JobConfiguration for generic_command."""

from jade.jobs.job_container_by_key import JobContainerByKey
from jade.jobs.job_configuration import JobConfiguration
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

    def iter_jobs(self):
        # Overrides JobConfiguration.iter_jobs so that it delivers jobs in
        # order of job_id.
        for job in self._jobs.get_jobs(sort_by_key=True):
            yield job

    def create_from_result(self, job, output_dir):
        return None

    def get_job_inputs(self):
        return self._inputs
