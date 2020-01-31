"""Contains class for running any post process scripts available"""

import os
import logging

from prettytable import PrettyTable

from jade.common import JOBS_OUTPUT_DIR
from jade.utils.utils import load_data, output_to_file

logger = logging.getLogger(__name__)

class JobPostProcess:
    """Class used to dynamically run post process scripts"""
    _output_dir = f"output/{JOBS_OUTPUT_DIR}"
    _results_file = "post-process-results.json"

    def __init__(self, module_name, class_name, data=None, job_name=None, **kwargs):
        """Constructs JobPostProcess

        Parameters
        ----------
        module_name : str
        class_name : str
        data : dict

        """
        self._data = data
        self._job_name = job_name

        try:
            # dynamically get class from analysis module
            process_module = __import__(module_name, fromlist=[class_name])
            process_class = getattr(process_module, class_name)
            self._post_process = process_class(self._data, self._job_name, **kwargs)
        except ModuleNotFoundError as module_error:
            logger.exception(module_error)
        except ValueError as value_error:
            logger.exception(value_error)

    @classmethod
    def load_config_from_file(cls, config_file):
        """Loads config from given toml file

        Parameters
        ----------
        config_file : str

        Returns
        -------
        module_name : str
        class_name : str
        data : str

        """

        config = load_data(config_file)
        module_name = None
        class_name = None
        data = {}

        if 'module' in config.keys():
            module_name = config['module']

        if 'class' in config.keys():
            class_name = config['class']

        if 'data' in config.keys():
            for data_index in config['data']:
                data[data_index] = config['data'][data_index]

        return module_name, class_name, data

    def run(self, *kwargs):
        """Runs post-process class' run function"""
        self._post_process.run(*kwargs)
        self._dump_results()

    def serialize(self):
        """Create data for serialization."""
        serialized_data = {
            "class": self._post_process.__class__.__name__,
            "module": self._post_process.__module__
        }

        if self._data is not None:
            serialized_data['data'] = self._data

        return serialized_data

    @classmethod
    def show_results(cls, job_name=None):
        """Show the post process results for jobs in a table.

        Parameters
        ----------
        job_name : str
            optional individual job to display
        """

        print(f"Post-process results from directory: {cls._output_dir}")

        job_names = None
        for _, dirs, _ in os.walk(cls._output_dir):
            job_names = dirs
            break

        table = PrettyTable()

        for job in job_names:
            # skip job if not given job name
            if job_name is not None and job_name != job:
                continue

            results = load_data(f"{cls._output_dir}/{job}/{cls._results_file}")

            if not table.field_names:
                table.field_names = [ 'job' ] + list(results[0].keys())

            for result in results:
                row = [ job ]
                for column in result:
                    row.append(result[column])

                table.add_row(row)

        print(table)

    def _dump_results(self):
        results = self._post_process.get_results()
        output_to_file(results, f"{self._output_dir}/{self._job_name}/{self._results_file}")

    def dump(self):
        """Outputs post process data to results file"""
        output_to_file(self.serialize(), self._results_file)
