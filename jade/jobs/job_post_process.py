"""Contains class for running any post process scripts available"""

import os
import logging

from prettytable import PrettyTable

from jade.common import JOBS_OUTPUT_DIR, OUTPUT_DIR
from jade.utils.utils import load_data, output_to_file

logger = logging.getLogger(__name__)


class JobPostProcess:
    """Class used to dynamically run post process scripts"""

    _results_file = "post-process-results.json"

    def __init__(
        self, module_name, class_name, data=None, output=OUTPUT_DIR, job_name=None, *args, **kwargs
    ):
        """Constructs JobPostProcess

        Parameters
        ----------
        module_name : str
            module which contains class that needs to run
        class_name : str
            class implementing post process to run
        data : dict
            optional dictionary of overrides data to send to post process

        """
        self._data = data or {}
        self._job_name = job_name
        self._output = output

        try:
            process_module = __import__(module_name, fromlist=[class_name])
            process_class = getattr(process_module, class_name)
            self._post_process = process_class(overrides=self._data, job_name=self._job_name)
        except ModuleNotFoundError:
            logger.exception("Could not import module %s", module_name)
            raise
        except ValueError:
            logger.exception("Module %s does not have class %s", module_name, class_name)
            raise

    def run(self, *args, **kwargs):
        """Runs post-process class' run function"""
        self._post_process.run(*args, **kwargs)
        self._dump_results()

    def serialize(self):
        """Create data for serialization."""
        serialized_data = {
            "class": self._post_process.__class__.__name__,
            "module": self._post_process.__module__,
        }

        data = self._post_process.serialized_data
        if data is not None:
            serialized_data["data"] = data

        return serialized_data

    def dump_config(self, output_file="post-process-config.toml"):
        """Outputs post process data to results file

        Parameters
        ----------
        output_file : str
        """
        output_to_file(self.serialize(), output_file)

    def _get_job_results_dir(self):
        return os.path.join(self._output, self._job_name)

    def _dump_results(self):
        results = self._post_process.get_results()
        output_path = os.path.join(self._get_job_results_dir(), self._results_file)
        data = {
            "job": self._job_name,
            "post-process": type(self._post_process).__name__,
            "results": results,
        }
        logger.info("Dumping post-process results to %s", output_path)
        output_to_file(data, output_path)

    @classmethod
    def load_config_from_file(cls, config_file):
        """Loads config from given toml file

        Parameters
        ----------
        config_file : str

        Returns
        -------
        module_name : str
            module which contains class that needs to run
        class_name : str
            class implementing post process to run
        data : dict
            optional dictionary of additional data to send to post process

        """

        config = load_data(config_file)
        module_name = None
        class_name = None
        data = {}

        if "module" in config.keys():
            module_name = config["module"]

        if "class" in config.keys():
            class_name = config["class"]

        if "data" in config.keys():
            for data_index in config["data"]:
                data[data_index] = config["data"][data_index]

        return module_name, class_name, data

    @classmethod
    def show_results(cls, output_dir, job_name=None, input_file=None):
        """Show the post process results for jobs in a table.
           Expects that JOBS_OUTPUT_DIR contains folders named after
           jobs containing results.

        Parameters
        ----------
        job_name : str
            optional individual job to display
        input_file : str
            optional input file name
        """

        if input_file is None:
            input_file = cls._results_file

        job_results_dir = os.path.join(output_dir, JOBS_OUTPUT_DIR)
        print(f"Post-process results from directory: {job_results_dir}")
        job_names = None

        for _, dirs, _ in os.walk(job_results_dir):
            job_names = dirs
            assert job_names
            break

        table = PrettyTable()

        for job in job_names:
            if job_name is not None and job_name != job:
                continue

            post_process_results = load_data(os.path.join(job_results_dir, job, input_file))
            results = post_process_results["results"]["outputs"]
            if not table.field_names and results:
                table.field_names = ["job"] + list(results[0].keys())

            for result in results:
                row = [job]
                for column in result:
                    row.append(result[column])

                table.add_row(row)

        print(table)
