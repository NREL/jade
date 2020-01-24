"""Contains class for running any post process scripts available"""

import logging

from jade.utils.utils import load_data

logger = logging.getLogger(__name__)

class JobPostProcess:
    """Class used to dynamically run post process scripts"""

    def __init__(self, module_name, class_name, data):
        """Constructs JobPostProcess

        Parameters
        ----------
        module_name : str
        class_name : str
        data : dict

        """
        self._data = data

        try:
            # dynamically get class from analysis module
            process_module = __import__(module_name, fromlist=[class_name])
            process_class = getattr(process_module, class_name)
            self._post_process = process_class(self._data)
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
