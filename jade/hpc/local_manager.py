"""SLURM management functionality"""

import logging
import multiprocessing
import tempfile

from jade.hpc.common import HpcJobStatus, HpcJobInfo
from jade.hpc.hpc_manager_interface import HpcManagerInterface


logger = logging.getLogger(__name__)


DEFAULTS = {
    "walltime": 60 * 12,
    "interface": "ib0",
    "local_directory": tempfile.gettempdir(),
    "memory": 5000,
}


class LocalManager(HpcManagerInterface):
    """Manages local execution of jobs."""

    _OPTIONAL_CONFIG_PARAMS = {}
    _REQUIRED_CONFIG_PARAMS = []
    _STATUSES = {
        "PD": HpcJobStatus.QUEUED,
        "R": HpcJobStatus.RUNNING,
        "CG": HpcJobStatus.COMPLETE,
    }

    def __init__(self, _):
        pass

    def cancel_job(self, job_id):
        return 0

    def check_status(self, name=None, job_id=None):
        return HpcJobInfo("", "", HpcJobStatus.NONE)

    def check_storage_configuration(self):
        pass

    def create_cluster(self):
        pass

    def create_local_cluster(self):
        pass

    def create_submission_script(self, name, script, filename, path):
        pass

    def get_config(self):
        return {"hpc": {}}

    def get_local_scratch(self):
        return tempfile.gettempdir()

    @staticmethod
    def get_num_cpus():
        return multiprocessing.cpu_count()

    def get_optional_config_params(self):
        return self._OPTIONAL_CONFIG_PARAMS

    def get_required_config_params(self):
        return self._REQUIRED_CONFIG_PARAMS

    def log_environment_variables(self):
        pass

    def submit(self, filename):
        return 0
