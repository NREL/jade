import logging
import os

from jade.jobs.job_execution_interface import JobExecutionInterface


BATCH_POST_PROCESS_OUTPUT = "batch-post-process-outputs"

logger = logging.getLogger(__name__)


class BatchPostProcessExecution(JobExecutionInterface):
    """
    A class used for run batch post-process script.
    """
    def __init__(self, job, output):
        """
        Init auto-regression execution class

        Parameters
        ----------
        job: :obj:`BatchPostProcessParameters`
            The instance of :obj:`AutoRegressionParameters`
        output: str,
            The path to the output directory.
        """
        self._job = job
        self._output = output

    @property
    def results_directory(self):
        """Return the results directory created by the simulation."""
        return self._output

    @classmethod
    def create(cls, _, job, output, **kwargs):
        """Create instance of :obj:`AutoRegressionExecution`"""
        return cls(job, output)

    @staticmethod
    def generate_command(job, output, config_file, verbose=False):
        """
        Generate command consumed by bash for running auto-regression analysis.

        Parameters
        ----------
        job: :obj:`BatchPostProcessParameters`
            The instance of :obj:`BatchPostProcessParameters`.
        output: str
            The path to the output directory.
        config_file: str,
            The path to the configuration json file of job inputs.
        verbose: bool
            True if verbose, otherwise False.

        Returns
        -------
            str, A command line string
        """
        extension = "batch_post_process"
        command = [
            f"jade-internal run {extension}",
            f"--name={job.name}",
            f"--output={output}",
            f"--config-file={config_file}",
        ]

        if verbose:
            command.append("--verbose")

        return " ".join(command)

    def post_process(self, **kwargs):
        pass

    def list_results_files(self):
        """Return a list of result filenames created by the simulation."""
        return [
            os.path.join(self._output, x)
            for x in os.listdir(self._output)
        ]

    def run(self):
        """Runs the batch post-process, and return status code"""
        params = self._job.serialize()
        module_name = params["module"]
        class_name = params["class_name"]
        data = params["data"]
        try:
            process_module = __import__(module_name, fromlist=[class_name])
            process_class = getattr(process_module, class_name)
            batch_post_process = process_class()
        except ModuleNotFoundError as e:
            logger.exception(e)
            raise
        except ValueError as e:
            logger.exception(e)
            raise

        output = os.path.dirname(self._output)
        batch_post_process_output = os.path.join(output, BATCH_POST_PROCESS_OUTPUT)
        ret = batch_post_process.run(data=data, output=batch_post_process_output)

        return ret
