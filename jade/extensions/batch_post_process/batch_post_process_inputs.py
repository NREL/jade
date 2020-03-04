import os
from concurrent.futures import ProcessPoolExecutor

from jade.common import JOBS_OUTPUT_DIR
from jade.jobs.job_inputs_interface import JobInputsInterface
from jade.utils.utils import load_data

from jade.extensions.batch_post_process.batch_post_process_parameters import \
    BatchPostProcessParameters


class BatchPostProcessInputs(JobInputsInterface):
    """
    A class used for configuring batch post-process jobs.
    """
    POST_PROCESS_RESULTS = "post-process-results.json"

    def __init__(self, base_directory, batch_post_process_config):
        """
        Init class

        Parameters
        ----------
        base_directory: str
            the output directory of task.
        batch_post_process_config: dict
            the batch post-process config.
        """
        self._batch_post_process_config = batch_post_process_config
        self._base_directory = base_directory
        self._parameters = {}

    @property
    def base_directory(self):
        """Return the base directory."""
        return self._base_directory

    def _get_post_process_results(self, job_name):
        """Get post-process-results file"""
        results_file = os.path.join(
            self.base_directory,
            JOBS_OUTPUT_DIR,
            job_name,
            self.POST_PROCESS_RESULTS
        )
        if not os.path.exists(results_file):
            return {}

        data = load_data(results_file)
        return {job_name: data["results"]["outputs"]}

    def get_available_parameters(self, **kwargs):
        """Return a dictionary containing all available parameters."""
        jobs_output_dir = os.path.join(self.base_directory, JOBS_OUTPUT_DIR)
        job_names = os.listdir(jobs_output_dir)

        num_workers = kwargs.get("num_workers", 2)
        jobs_post_process_results = {}
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            results = executor.map(self._get_post_process_results, job_names)

        for result in list(results):
            jobs_post_process_results.update(result)

        module_name = self._batch_post_process_config.get("module", None)
        class_name = self._batch_post_process_config.get("class_name", None)
        job_param = BatchPostProcessParameters(
            module_name=module_name,
            class_name=class_name,
            data=jobs_post_process_results
        )
        self._parameters[job_param.name] = job_param

    def iter_jobs(self):
        return list(self._parameters.values())
