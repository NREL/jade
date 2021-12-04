"""SLURM management functionality"""

import logging
import multiprocessing
import tempfile
import time
from pathlib import Path

from filelock import SoftFileLock

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

    JOB_ID_FILE = "fake_manager_job_id.txt"
    LOCK_FILE = "fake_manager.lock"

    def __init__(self, config):
        self._subprocess_mgr = None
        self._job_id = None
        self._config = config

    def am_i_manager(self):
        return True

    def cancel_job(self, job_id):
        return 0

    def check_status(self, name=None, job_id=None):
        if self._subprocess_mgr is None:
            job_info = HpcJobInfo(job_id, "", HpcJobStatus.NONE)
        elif self._subprocess_mgr.in_progress():
            job_info = HpcJobInfo(job_id, "", HpcJobStatus.RUNNING)
        else:
            job_info = HpcJobInfo(job_id, "", HpcJobStatus.COMPLETE)

        logger.debug("status=%s", job_info)
        return job_info

    def check_statuses(self):
        val = {self._job_id: self.check_status(job_id=self._job_id).status}
        return val

    def check_storage_configuration(self):
        pass

    def create_cluster(self):
        pass

    def create_local_cluster(self):
        pass

    def create_submission_script(self, name, script, filename, path):
        lines = [
            "#!/bin/bash",
            str(script),
        ]
        create_script(filename, "\n".join(lines))

    def get_config(self):
        return {"hpc": {}}

    def get_current_job_id(self):
        return None

    def get_local_scratch(self):
        return tempfile.gettempdir()

    def get_node_id(self):
        # If we try to use multi-node jobs in fake mode, this will cause a problem.
        return "0"

    @staticmethod
    def get_num_cpus():
        return multiprocessing.cpu_count()

    def list_active_nodes(self, job_id):
        assert False

    def log_environment_variables(self):
        pass

    @staticmethod
    def _get_next_job_id(output_path):
        """Returns the next job ID and increments the index.
        A lock must be held while calling this method.

        """
        # TODO: This could be enhanced to record completions.
        path = output_path / FakeManager.JOB_ID_FILE
        if path.exists():
            job_id = int(path.read_text().strip())
        else:
            job_id = 1
        next_job_id = job_id + 1
        path.write_text(str(next_job_id) + "\n")
        return job_id

    def submit(self, filename):
        # This method has a workaround for problems seen on some Linux systems
        # (never on Mac).
        # When multiple processes call this method at about the same time,
        # one or more of the subprocesses do not get started. It seems like
        # something within Python gets locked up.
        # This workaround staggers starting of the subprocesses and prevents
        # the issue from occurring.
        output_path = Path(filename).parent
        with SoftFileLock(output_path / FakeManager.LOCK_FILE, timeout=30):
            self._job_id = self._get_next_job_id(output_path)
            self._subprocess_mgr = SubprocessManager()
            self._subprocess_mgr.run(filename)
            logger.info("Submit job with %s", self._job_id)
            time.sleep(1)
            return Status.GOOD, self._job_id, None
