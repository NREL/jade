"""HPC management functionality"""

import logging
import os
import time

from jade.enums import Status
from jade.exceptions import InvalidParameter, ExecutionError
from jade.hpc.common import HpcType, HpcJobStatus
from jade.hpc.fake_manager import FakeManager
from jade.hpc.local_manager import LocalManager
from jade.hpc.pbs_manager import PbsManager
from jade.hpc.slurm_manager import SlurmManager
from jade.jobs.async_job_interface import AsyncJobInterface
from jade.utils.utils import load_data


logger = logging.getLogger(__name__)


class HpcManager:
    """Manages HPC job submission and monitoring."""
    def __init__(self, config_file, output):
        self._intf, self._hpc_type = self._create_hpc_manager(config_file)
        self._output = output
        self._num_in_progress = 0

        logger.debug("Constructed HpcManager with output=%s", output)

    def cancel_job(self, job_id):
        """Cancel job.

        Parameters
        ----------
        job_id : str

        Returns
        -------
        int
            return code

        """
        ret = self._intf.cancel_job(job_id)
        if ret == 0:
            logger.info("Successfully cancelled job ID %s", job_id)
        else:
            logger.info("Failed to cancel job ID %s", job_id)

        return ret

    def check_status(self, name=None, job_id=None):
        """Return the status of a job by name or ID.

        Parameters
        ----------
        name : str
            job name
        job_id : str
            job ID

        Returns
        -------
        HpcJobInfo
            job info

        """
        if (name is None and job_id is None) or \
           (name is not None and job_id is not None):
            raise InvalidParameter("exactly one of name / job_id must be set")

        info = self._intf.check_status(name=name, job_id=job_id)
        logger.debug("info=%s", info)
        return info.status

    def get_hpc_config(self):
        """Returns the HPC config parameters.

        Returns
        -------
        dict
            config parameters

        """
        return self._intf.get_config()

    @property
    def hpc_type(self):
        """Return the type of HPC management system.

        Returns
        -------
        HpcType

        """
        return self._hpc_type

    def submit(self, directory, name, script, wait=False,
               keep_submission_script=True):
        """Submits scripts to the queue for execution.

        Parameters
        ----------
        directory : str
            directory to contain the submission script
        name : str
            job name
        script : str
            Script to execute.
        wait : bool
            Wait for execution to complete.
        keep_submission_script : bool
            Do not delete the submission script.

        Returns
        -------
        tuple
            (job_id, submission status)

        """
        self._intf.check_storage_configuration()

        # TODO: enable this logic if batches have unique names.
        #info = self._intf.check_status(name=name)
        #if info.status in (HpcJobStatus.QUEUED, HpcJobStatus.RUNNING):
        #    raise JobAlreadyInProgress(
        #        "Not submitting job '{}' because it is already active: "
        #        f"{info}"
        #    )

        filename = os.path.join(directory, name + ".sh")
        self._intf.create_submission_script(name, script, filename,
                                            self._output)
        logger.info("Created submission script %s", filename)
        result, job_id, err = self._intf.submit(filename)

        if result == Status.GOOD:
            logger.info("job '%s' with ID=%s submitted successfully", name,
                        job_id)
            if not keep_submission_script:
                os.remove(filename)
        else:
            logger.error("Failed to submit job '%s': result=%s: %s", name,
                         result, err)

        if wait:
            self._wait_for_completion(job_id)

        return job_id, result

    @staticmethod
    def _create_hpc_manager(config_file):
        """Returns an HPC implementation instance appropriate for the current
        environment.

        """
        cluster = os.environ.get("NREL_CLUSTER")
        if cluster is None:
            if os.environ.get("FAKE_HPC_CLUSTER") is not None:
                intf = FakeManager(config_file)
                hpc_type = HpcType.FAKE
            else:
                intf = LocalManager(config_file)
                hpc_type = HpcType.LOCAL
        elif cluster == "peregrine":
            intf = PbsManager(config_file)
            hpc_type = HpcType.PBS
        elif cluster == "eagle":
            intf = SlurmManager(config_file)
            hpc_type = HpcType.SLURM
        else:
            raise ValueError("Unsupported cluster type: {}".format(cluster))

        logger.debug("HPC manager type=%s", hpc_type)
        return intf, hpc_type

    def _wait_for_completion(self, job_id):
        status = HpcJobStatus.UNKNOWN

        while status not in (HpcJobStatus.COMPLETE, HpcJobStatus.NONE):
            time.sleep(5)
            job_info = self._intf.check_status(job_id=job_id)
            logger.debug("job_info=%s", job_info)
            if job_info.status != status:
                logger.info("Status of job ID %s changed to %s",
                            job_id, job_info.status)
                status = job_info.status

        logger.info("Job ID %s is complete", job_id)


class AsyncHpcSubmitter(AsyncJobInterface):
    """Used to submit batches of jobs to multiple nodes, one at a time."""
    def __init__(self, hpc_manager, run_script, name, output):
        self._mgr = hpc_manager
        self._run_script = run_script
        self._job_id = None
        self._output = output
        self._name = name
        self._last_status = HpcJobStatus.NONE
        self._is_pending = False

    def __del__(self):
        if self._is_pending:
            logger.warning("job %s destructed while pending", self._name)

    @property
    def hpc_manager(self):
        """Return the HpcManager object.

        Returns
        -------
        HpcManager

        """
        return self._mgr

    def is_complete(self):
        status = self._mgr.check_status(job_id=self._job_id)

        if status != self._last_status:
            logger.info("Submission %s %s changed status from %s to %s",
                        self._name, self._job_id, self._last_status, status)
            self._last_status = status

        if status in (HpcJobStatus.COMPLETE, HpcJobStatus.NONE):
            self._is_pending = False

        return not self._is_pending

    def name(self):
        return self._name

    def run(self):
        job_id, result = self._mgr.submit(self._output,
                                          self._name,
                                          self._run_script)
        self._is_pending = True
        if result != Status.GOOD:
            raise ExecutionError("Failed to submit name={self._name}")

        self._job_id = job_id
        logger.info("Assigned job_ID=%s name=%s", self._job_id, self._name)
