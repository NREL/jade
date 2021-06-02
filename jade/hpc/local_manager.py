"""SLURM management functionality"""

import logging
import multiprocessing
import tempfile

from jade.enums import Status
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

    _STATUSES = {
        "PD": HpcJobStatus.QUEUED,
        "R": HpcJobStatus.RUNNING,
        "CG": HpcJobStatus.COMPLETE,
    }

    def __init__(self, _):
        pass

    def am_i_manager(self):
        return True

    def cancel_job(self, job_id):
        return 0

    def check_status(self, name=None, job_id=None):
        return HpcJobInfo("", "", HpcJobStatus.NONE)

    def check_statuses(self):
        return {}

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

    def get_node_id(self):
        return "0"

    @staticmethod
    def get_num_cpus():
        return multiprocessing.cpu_count()

    def list_active_nodes(self, job_id):
        assert False

    def log_environment_variables(self):
        pass

    def submit(self, filename):
        return Status.GOOD, 1, None
