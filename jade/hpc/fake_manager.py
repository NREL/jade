"""SLURM management functionality"""

import logging
import multiprocessing
import os
import tempfile

from jade.enums import Status
from jade.hpc.common import HpcJobStatus, HpcJobInfo
from jade.hpc.hpc_manager_interface import HpcManagerInterface
from jade.utils.subprocess_manager import SubprocessManager
from jade.utils.utils import create_script


logger = logging.getLogger(__name__)


DEFAULTS = {
    "walltime": 60 * 12,
    "interface": "ib0",
    "local_directory": tempfile.gettempdir(),
    "memory": 5000,
}


class FakeManager(HpcManagerInterface):
    """Simulates management of HPC jobs."""

    _OPTIONAL_CONFIG_PARAMS = {}
    _REQUIRED_CONFIG_PARAMS = ()

    def __init__(self, _):
        self._subprocess_mgr = None

    def cancel_job(self, job_id):
        return 0

    def check_status(self, name=None, job_id=None):
        if self._subprocess_mgr is None:
            status = HpcJobInfo("", "", HpcJobStatus.NONE)
        elif self._subprocess_mgr.in_progress():
            status = HpcJobInfo("", "", HpcJobStatus.RUNNING)
        else:
            status = HpcJobInfo("", "", HpcJobStatus.COMPLETE)

        logger.debug("status=%s", status)
        return status

    def check_storage_configuration(self):
        pass

    def create_cluster(self):
        pass

    def create_local_cluster(self):
        pass

    def create_submission_script(self, name, script, filename, path):
        lines = [
            "#!/bin/bash",
            script,
        ]
        create_script(filename, "\n".join(lines))

    def get_config(self):
        return {"hpc": {}}

    def get_local_scratch(self):
        for envvar in ("TMP", "TEMP"):
            tmpdir = os.environ.get(envvar)
            if tmpdir:
                return tmpdir
        return "."

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
        self._subprocess_mgr = SubprocessManager()
        self._subprocess_mgr.run(filename)
        job_id = "1234"
        return Status.GOOD, job_id, None
