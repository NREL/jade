"""Generic batch post processor"""

import tempfile

from jade.extensions.generic_command.generic_command_inputs import GenericCommandInputs
from jade.extensions.generic_command.generic_command_configuration import GenericCommandConfiguration


class BatchPostProcess:
    """A class for configuring batch-post process commands.
    """

    def __init__(self, config_file):
        self._config_file = config_file
    
    @property
    def name(self):
        """The name of batch post-process"""
        return "batch-post-process"
    
    def auto_config(self):
        """Auto config by using generic_command extension."""
        inputs = GenericCommandInputs(self._config_file)
        config = GenericCommandConfiguration(job_inputs=inputs)
        for job_param in inputs.iter_jobs():
            config.add_job(job_param)
        return config
    
    def serialize(self):
        """Serialize batch post-process object"""
        data = {
            "type": "Commands",
            "file": self._config_file
        }
        return data
