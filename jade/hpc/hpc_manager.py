"""HPC management functionality"""

import logging
import os
import time

from jade.enums import Status
from jade.exceptions import InvalidParameter
from jade.hpc.common import HpcType, HpcJobStatus
from jade.hpc.fake_manager import FakeManager
from jade.hpc.local_manager import LocalManager
from jade.hpc.pbs_manager import PbsManager
from jade.hpc.slurm_manager import SlurmManager
from jade.models import HpcConfig


logger = logging.getLogger(__name__)


class HpcManager:
    """Manages HPC job submission and monitoring."""
    def __init__(self, config, output):
        self._config = config
        self._intf = self._create_hpc_interface(config)
        self._output = output

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
        HpcJobStatus

        """
        if (name is None and job_id is None) or \
           (name is not None and job_id is not None):
            raise InvalidParameter("exactly one of name / job_id must be set")

        info = self._intf.check_status(name=name, job_id=job_id)
        logger.debug("info=%s", info)
        return info.status

    def check_statuses(self):
        """Check the statuses of all user jobs.

        Returns
        -------
        dict
            key is job_id, value is HpcJobStatus

        """
        return self._intf.check_statuses()

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
        return self._config.hpc_type

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
    def _create_hpc_interface(config):
        """Returns an HPC implementation instance appropriate for the current
        environment.

        """
        if config.hpc_type is not None:
            if config.hpc_type == HpcType.SLURM:
                intf = SlurmManager(config)
            elif config.hpc_type == HpcType.FAKE:
                intf = FakeManager(config)
            elif config.hpc_type == HpcType.LOCAL:
                intf = LocalManager(config)
            else:
                raise ValueError("Unsupported HPC type: {}".format(config.hpc_type))

            logger.debug("HPC manager type=%s", config.hpc_type)
            return intf

        cluster = os.environ.get("NREL_CLUSTER")
        if cluster is None:
            if os.environ.get("FAKE_HPC_CLUSTER") is not None:
                intf = FakeManager(config)
                config.hpc_type = HpcType.FAKE
            else:
                intf = LocalManager(config)
                config.hpc_type = HpcType.LOCAL
        elif cluster == "peregrine":
            intf = PbsManager(config)
            config.hpc_type = HpcType.PBS
        elif cluster == "eagle":
            intf = SlurmManager(config)
            config.hpc_type = HpcType.SLURM
        else:
            raise ValueError("Unsupported HPC type: {}".format(cluster))

        logger.debug("HPC manager type=%s", config.hpc_type)
        return intf

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
