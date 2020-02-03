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
    def show_results(cls, output_dir, job_name=None, input_file=None):
        """Show the post process results for jobs in a table.

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

            results = load_data(os.path.join(job_results_dir, job, input_file))

            if not table.field_names:
                table.field_names = ['job'] + list(results[0].keys())

            for result in results:
                row = [job]
                for column in result:
                    row.append(result[column])

                table.add_row(row)

        print(table)

    def _dump_results(self, output_dir=None):
        if output_dir is None:
            output_dir = os.path.join(OUTPUT_DIR, JOBS_OUTPUT_DIR)

        results = self._post_process.get_results()
        output_to_file(results, os.path.join(output_dir, self._job_name, self._results_file))

    def dump(self, output_file=None):
        """Outputs post process data to results file

        Parameters
        ----------
        output_file : str
        """
        if output_file is None:
            output_file = self._results_file
        output_to_file(self.serialize(), output_file)
