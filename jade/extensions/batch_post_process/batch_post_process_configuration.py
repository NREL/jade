from jade.jobs.job_container_by_key import JobContainerByKey
from jade.jobs.job_configuration import JobConfiguration
from jade.utils.utils import load_data
from jade.extensions.batch_post_process.batch_post_process_inputs import \
    BatchPostProcessInputs
from jade.extensions.batch_post_process.batch_post_process_parameters import \
    BatchPostProcessParameters


class BatchPostProcessConfiguration(JobConfiguration):

    def __init__(self, inputs, **kwargs):
        batch_post_process_config = kwargs.pop("batch_post_process_config", None)

        super(BatchPostProcessConfiguration, self).__init__(
            inputs=inputs,
            container=JobContainerByKey(),
            job_parameters_class=BatchPostProcessParameters,
            extension_name="batch_post_process",
            batch_post_process_config=batch_post_process_config,
            **kwargs
        )

    def _serialize(self, data):
        pass

    def create_from_result(self, job, output_dir):
        pass

    def get_job_inputs(self):
        return self.inputs

    @classmethod
    def create_config_from_file(cls, base_directory, config_file, **kwargs):
        """
        Create BatchPostProcessConfiguration instance from master config file.

        Parameters
        ----------
        base_directory: str
            The output directory of task.
        config_file: str
            The path of master config file

        Returns
        -------
        :obj:`BatchPostProcessConfiguration`
            The BatchPostProcessConfiguration instance.
        """
        data = load_data(config_file)
        batch_post_process_config = data.get("batch_post_process_config", None)

        inputs = BatchPostProcessInputs(base_directory, batch_post_process_config)
        inputs.get_available_parameters(num_workers=kwargs.get("num_workers", 2))
        config = cls(inputs=inputs, batch_post_process_config=batch_post_process_config)

        for job in inputs.iter_jobs():
            config.add_job(job)

        return config
