import logging
from jade.jobs.job_parameters_interface import JobParametersInterface


BATCH_POST_PROCESS_NAME = "batch_post_process"


logger = logging.getLogger(__name__)


class BatchPostProcessParameters(JobParametersInterface):
    """
    A class used for configuring batch post-process job.
    """

    def __init__(self, module_name, class_name, data):
        """Init batch post-process parameters class.

        Parameters
        ----------
        module_name: str
            the module which contains batch post-process class
        class_name: str
            the class which is used for executing batch post-process.
        data : list
            A list of batch post-process results.
        """
        self._module_name = module_name
        self._class_name = class_name
        self._data = data

    def __str__(self):
        return "<BatchPostProcessParameters: {}>".format(self.name)

    @property
    def module_name(self):
        return self._module_name

    @property
    def class_name(self):
        return self._class_name

    @property
    def name(self):
        return BATCH_POST_PROCESS_NAME

    def serialize(self):
        return {
            "module": self._module_name,
            "class_name": self._class_name,
            "data": self._data
        }

    @classmethod
    def deserialize(cls, params):
        module_name = params.get("module", None)
        class_name = params.get("class_name", None)
        data = params.get("data", [])
        return cls(module_name, class_name, data)
