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

    def __init__(self, submission_groups, output):
        self._output = output
        self._configs = {}
        self._intfs = {}
        self._hpc_type = None
        assert submission_groups
        for name, group in submission_groups.items():
            self._configs[name] = group.submitter_params.hpc_config
            self._intfs[name] = self.create_hpc_interface(group.submitter_params.hpc_config)
            if self._hpc_type is None:
                self._hpc_type = group.submitter_params.hpc_config.hpc_type

        logger.debug("Constructed HpcManager with output=%s", output)

    def _get_interface(self, submission_group_name=None):
        if submission_group_name is None:
            # In many cases we don't care which interface is used.
            # We could store job IDs by group if we need to perform actions by group
            # in the future.
            # As of now we don't track IDs at all in this class.
            return next(iter(self._intfs.values()))
        return self._intfs[submission_group_name]

    def am_i_manager(self):
        """Return True if the current node is the master node.

        Returns
        -------
        bool

        """
        intf = self._get_interface()
        return intf.am_i_manager()

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
        intf = self._get_interface()
        ret = intf.cancel_job(job_id)
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
        if (name is None and job_id is None) or (name is not None and job_id is not None):
            raise InvalidParameter("exactly one of name / job_id must be set")

        intf = self._get_interface()
        info = intf.check_status(name=name, job_id=job_id)
        logger.debug("info=%s", info)
        return info.status

    def check_statuses(self):
        """Check the statuses of all user jobs.

        Returns
        -------
        dict
            key is job_id, value is HpcJobStatus

        """
        intf = self._get_interface()
        return intf.check_statuses()

    def get_hpc_config(self, submission_group_name):
        """Returns the HPC config parameters.

        Parameters
        ----------
        submission_group_name : str

        Returns
        -------
        dict
            config parameters

        """
        return self._get_interface(submission_group_name=submission_group_name)

    @property
    def hpc_type(self):
        """Return the type of HPC management system.

        Returns
        -------
        HpcType

        """
        return self._hpc_type

    def list_active_nodes(self, job_id):
        """Return the nodes currently participating in the job.

        Parameters
        ----------
        job_id : str

        Returns
        -------
        list
            list of node hostnames

        """
        intf = self._get_interface()
        return intf.list_active_nodes(job_id)

    def submit(
        self,
        directory,
        name,
        script,
        submission_group_name,
        wait=False,
        keep_submission_script=True,
        dry_run=False,
    ):
        """Submits scripts to the queue for execution.

        Parameters
        ----------
        directory : str
            directory to contain the submission script
        name : str
            job name
        script : str
            Script to execute.
        submission_group_name : str
        wait : bool
            Wait for execution to complete.
        keep_submission_script : bool
            Do not delete the submission script.
        dry_run : bool
            Do not actually submit jobs. Just create the files.

        Returns
        -------
        tuple
            (job_id, submission status)

        """
        intf = self._get_interface(submission_group_name)
        intf.check_storage_configuration()

        # TODO: enable this logic if batches have unique names.
        # info = intf.check_status(name=name)
        # if info.status in (HpcJobStatus.QUEUED, HpcJobStatus.RUNNING):
        #    raise JobAlreadyInProgress(
        #        "Not submitting job '{}' because it is already active: "
        #        f"{info}"
        #    )

        filename = os.path.join(directory, name + ".sh")
        intf.create_submission_script(name, script, filename, self._output)
        logger.info("Created submission script %s", filename)

        if dry_run:
            logger.info("Dry run mode enabled. Return without submitting.")
            return 0, Status.GOOD

        result, job_id, err = intf.submit(filename)

        if result == Status.GOOD:
            logger.info("job '%s' with ID=%s submitted successfully", name, job_id)
            if not keep_submission_script:
                os.remove(filename)
        else:
            logger.error("Failed to submit job '%s': result=%s: %s", name, result, err)

        if wait:
            self._wait_for_completion(job_id)

        return job_id, result

    @staticmethod
    def create_hpc_interface(config):
        """Returns an HPC implementation instance appropriate for the current
        environment.

        """
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

    def _wait_for_completion(self, job_id):
        status = HpcJobStatus.UNKNOWN
        intf = self._get_interface()

        while status not in (HpcJobStatus.COMPLETE, HpcJobStatus.NONE):
            time.sleep(5)
            job_info = intf.check_status(job_id=job_id)
            logger.debug("job_info=%s", job_info)
            if job_info.status != status:
                logger.info("Status of job ID %s changed to %s", job_id, job_info.status)
                status = job_info.status

        logger.info("Job ID %s is complete", job_id)
