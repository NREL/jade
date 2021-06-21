import logging
import os
import socket
from pathlib import Path

from filelock import SoftFileLock, Timeout

from jade.exceptions import InvalidConfiguration
from jade.jobs.job_configuration import JobConfiguration
from jade.models import ClusterConfig, Job, JobState, JobStatus
from jade.utils.utils import load_data


logger = logging.getLogger(__name__)


LOCK_TIMEOUT = 60


class Cluster:
    """Represents the state of the nodes running jobs."""

    CLUSTER_CONFIG_FILE = "cluster_config.json"
    JOB_STATUS_FILE = "job_status.json"
    LOCK_FILE = "cluster_config.json.lock"
    CONFIG_VERSION_FILE = "config_version.txt"
    JOB_STATUS_VERSION_FILE = "job_status_version.txt"

    def __init__(self, config, job_status=None, lock_timeout=LOCK_TIMEOUT):
        """Internal constructor. Use create() or deserialize()."""
        self._config = config
        self._timeout = lock_timeout
        self._job_status = job_status
        self._config_hash = None
        self._job_status_hash = None
        self._config_file = self.get_config_file(self._config.path)
        self._job_status_file = self.get_job_status_file(self._config.path)
        self._lock_file = self.get_lock_file(self._config.path)
        self._hostname = socket.gethostname()

        # These two files contain versions that are duplicated with the files
        # above. They exist to allow very quick reads to verify updates.
        self._config_version_file = os.path.join(
            config.path,
            self.CONFIG_VERSION_FILE,
        )
        self._job_status_version_file = os.path.join(
            config.path,
            self.JOB_STATUS_VERSION_FILE,
        )

    @classmethod
    def create(cls, path, jade_config: JobConfiguration, pipeline_stage_num=None):
        """Create a new instance of a Cluster. Promotes itself to submitter.

        Parameters
        ----------
        path : str
            Base directory for a JADE submission
        jade_config : JobConfiguration
        pipeline_stage_num : None | int
            True if the config is one stage of a pipeline

        """
        config = ClusterConfig(
            path=path,
            pipeline_stage_num=pipeline_stage_num,
            num_jobs=jade_config.get_num_jobs(),
            submitter=socket.gethostname(),
            submission_groups=jade_config.submission_groups,
            version=0,
        )
        job_status = JobStatus(
            jobs=[
                Job(
                    name=x.name,
                    blocked_by=x.get_blocking_jobs(),
                    cancel_on_blocking_job_failure=x.cancel_on_blocking_job_failure,
                    state=JobState.NOT_SUBMITTED,
                )
                for x in jade_config.iter_jobs()
            ],
            hpc_job_ids=[],
            version=0,
        )
        cluster = cls(config, job_status=job_status)
        cluster._serialize_config_version()
        cluster._serialize_job_status_version()
        cluster.serialize("create")
        cluster.serialize_jobs("create")
        return cluster

    @classmethod
    def deserialize(cls, path, try_promote_to_submitter=False, deserialize_jobs=False):
        """Deserialize an existing Cluster from a file.

        Parameters
        ----------
        path : str
            Base directory for a JADE submission
        try_promote_to_submitter : bool
            Attempt to promote to submitter
        deserialize_jobs : bool
            Deserialize current job status

        Returns
        -------
        tuple
            cluster and a bool indicating whether promotion occurred

        """
        return cls.do_action_under_lock(
            path,
            cls._deserialize,
            path,
            try_promote_to_submitter=try_promote_to_submitter,
            deserialize_jobs=deserialize_jobs,
        )

    def are_all_jobs_complete(self):
        """Return True if all jobs are complete.

        Returns
        -------
        bool

        """
        return self._do_action_under_lock(self._are_all_jobs_complete)

    def all_jobs_submitted(self):
        """Return true if all jobs have been submitted."""
        return self._config.submitted_jobs == self._config.num_jobs

    def am_i_submitter(self):
        """Return True if the current system is the submitter."""
        return self._config.submitter == self._hostname

    @property
    def config(self):
        """Return the ClusterConfig"""
        return self._config

    @property
    def config_file(self):
        """Return the path to the cluster config file.

        Returns
        -------
        str

        """
        return self.get_config_file(self._config.path)

    def delete_files_internal(self):
        # This should not be used in normal circumstances.
        for filename in (
            self.CLUSTER_CONFIG_FILE,
            self.JOB_STATUS_FILE,
            self.CONFIG_VERSION_FILE,
            self.JOB_STATUS_VERSION_FILE,
        ):
            path = os.path.join(self.config.path, filename)
            os.remove(path)
            logger.debug("Deleted %s", path)

    def demote_from_submitter(self, serialize=True):
        """Clear the submitter, which must be the current system."""
        return self._do_action_under_lock(self._demote_from_submitter, serialize=serialize)

    def deserialize_jobs(self):
        """Deserialize the current job status."""
        return self._do_action_under_lock(self._deserialize_jobs)

    @staticmethod
    def do_action_under_lock(path, func, *args, **kwargs):
        """Run a function while holding the lock."""
        lock_file = Cluster.get_lock_file(path)
        return Cluster._do_action_under_lock_internal(lock_file, func, *args, **kwargs)

    @staticmethod
    def get_config_file(path):
        """Return the path to the cluster config file.

        Parameters
        ----------
        path : str
            Base directory for a JADE submission

        """
        return os.path.join(path, Cluster.CLUSTER_CONFIG_FILE)

    @staticmethod
    def get_job_status_file(path):
        """Return the path to the job status file.

        Parameters
        ----------
        path : str
            Base directory for a JADE submission

        """
        return os.path.join(path, Cluster.JOB_STATUS_FILE)

    @staticmethod
    def get_lock_file(path):
        """Return the path to the lock file for the cluster config.

        Parameters
        ----------
        path : str
            Base directory for a JADE submission

        """
        return os.path.join(path, Cluster.LOCK_FILE)

    def get_status_summary(self, include_jobs=False):
        """Return a dict that summarizes current status.

        Parameters
        ----------
        include_jobs : bool
            Whether to include individual job status

        Returns
        -------
        dict

        """
        not_submitted = self._config.num_jobs - self._config.submitted_jobs
        summary = {
            "is_complete": self.is_complete(),
            "is_canceled": self.is_canceled(),
            "num_jobs": self._config.num_jobs,
            "completed_jobs": self._config.completed_jobs,
            "not_submitted_jobs": not_submitted,
        }

        if include_jobs:
            summary["job_status"] = self._job_status.dict()

        return summary

    def has_submitter(self):
        """Return True if the config has a submitter assigned."""
        return self._config.submitter is not None

    def iter_jobs(self, state=None):
        """Yields each Job.

        Parameters
        ----------
        state : JobState
            If not None, only return jobs that match this state.

        Yields
        ------
        Job

        """
        assert self._job_status is not None
        for job in self._job_status.jobs:
            if state is not None and job.state != state:
                continue
            yield job

    def iter_hpc_job_ids(self):
        """Yields each Job.

        Yields
        ------
        str
            HPC job ID

        """
        assert self._job_status is not None
        for job_id in self._job_status.hpc_job_ids:
            yield job_id

    @property
    def job_status(self):
        """Return the JobStatus"""
        return self._job_status

    def is_canceled(self):
        """Return True if the submission is canceled."""
        return self._config.is_canceled

    def is_complete(self):
        """Return True if the submission is complete."""
        return self._config.is_complete

    def mark_complete(self, canceled=False):
        """Mark the submission as being complete.

        Parameters
        ----------
        canceled : bool
            Set to True if the submission was canceled.

        """
        return self._do_action_under_lock(self._mark_complete, canceled)

    def prepare_for_resubmission(self, jobs_to_resubmit, updated_blocking_jobs_by_name):
        """Reset the state of the cluster for resubmission of jobs.

        Parameters
        ----------
        jobs_to_resubmit : set
            job names that will be resubmitted
        updated_blocking_jobs_by_name : dict
            contains the blocking jobs for each job to be resubmitted

        """
        # Locking is not required for this function.
        assert self._config.is_complete
        self._config.is_complete = False
        self._config.submitted_jobs = self._config.num_jobs - len(jobs_to_resubmit)
        self._config.completed_jobs = 0

        for job in self.iter_jobs():
            if job.name in jobs_to_resubmit:
                job.state = JobState.NOT_SUBMITTED
                job.blocked_by = updated_blocking_jobs_by_name[job.name]
            elif job.state == JobState.DONE:
                self._config.completed_jobs += 1

        self._serialize("prepare_for_resubmission")
        self._serialize_jobs("prepare_for_resubmission")

    def promote_to_submitter(self, serialize=True):
        """Promote the current system to submitter.

        Returns
        -------
        bool
            Returns True if promotion was successful
        """
        return self._do_action_under_lock(self._promote_to_submitter, serialize=serialize)

    def serialize(self, reason):
        """Serialize the config to a file."""
        self._do_action_under_lock(self._serialize, reason)

    def serialize_jobs(self, reason):
        """Serialize the job status to a file."""
        self._do_action_under_lock(self._serialize_jobs, reason)

    def update_job_status(
        self,
        submitted_jobs,
        blocked_jobs,
        canceled_jobs,
        completed_job_names,
        hpc_job_ids,
        batch_index,
    ):
        """Update the job status in the config file.

        Parameters
        ----------
        submitted_jobs : list
            list of Job
        blocked_jobs : list
            list of Job
        canceled_jobs : list
            list of Job
        completed_job_names : set
            set of str
        hpc_job_ids : list
            list of str of newly submitted job IDs
        batch_index : int
            next batch index

        """
        self._do_action_under_lock(
            self._update_job_status,
            submitted_jobs,
            blocked_jobs,
            canceled_jobs,
            completed_job_names,
            hpc_job_ids,
            batch_index,
        )

    def _are_all_jobs_complete(self):
        for job in self.iter_jobs():
            if job.state != JobState.DONE:
                assert (
                    self._config.completed_jobs != self._config.num_jobs
                ), "completed={self._config.completed_jobs}"
                return False

        assert (
            self._config.completed_jobs == self._config.num_jobs
        ), "completed={self._config.completed_jobs}"
        return True

    def _demote_from_submitter(self, serialize=True):
        assert self.am_i_submitter(), self._config.submitter
        self._config.submitter = None
        if serialize:
            self._serialize("demote_from_submitter")

    @classmethod
    def _deserialize(cls, path, try_promote_to_submitter=False, deserialize_jobs=False):
        config_file = cls.get_config_file(path)
        if not os.path.isfile(config_file):
            raise InvalidConfiguration(f"{config_file} does not exist")

        config = ClusterConfig(**load_data(config_file))
        cluster = cls(config)
        promoted = False
        if try_promote_to_submitter:
            promoted = cluster._promote_to_submitter()
        if deserialize_jobs:
            cluster._deserialize_jobs()

        return cluster, promoted

    def _deserialize_jobs(self):
        data = load_data(self.get_job_status_file(self._config.path))
        self._job_status = JobStatus(**data)

    def _do_action_under_lock(self, func, *args, **kwargs):
        return self._do_action_under_lock_internal(
            self._lock_file, func, *args, timeout=self._timeout, **kwargs
        )

    @staticmethod
    def _do_action_under_lock_internal(lock_file, func, *args, timeout=LOCK_TIMEOUT, **kwargs):
        # Using this instead of FileLock because it will be used across nodes
        # on the Lustre filesystem.
        lock = SoftFileLock(lock_file, timeout=timeout)
        try:
            lock.acquire(timeout=timeout)
        except Timeout:
            # Picked a default value such that this should not trip. If it does
            # trip under normal circumstances then we need to reconsider this.
            logger.error(
                "Failed to acquire file lock %s within %s seconds. hostname=%s",
                lock_file,
                timeout,
                socket.gethostname(),
            )
            raise

        try:
            val = func(*args, **kwargs)
            lock.release()
            return val
        except Exception:
            lock.release()
            # SoftFileLock always deletes the file, so create it again.
            Path(lock_file).touch()
            logger.exception(
                "An exception occurred while holding the Cluster lock. "
                "The state of the cluster is unknown. A deadlock will occur."
            )
            raise

    def _get_config_version(self):
        with open(self._config_version_file, "r") as f_in:
            return int(f_in.read().strip())

    def _get_job_status_version(self):
        with open(self._job_status_version_file, "r") as f_in:
            return int(f_in.read().strip())

    def _mark_complete(self, canceled):
        assert not self._config.is_complete
        self._config.is_complete = True
        if canceled:
            self._config.is_canceled = True
        self._serialize("mark_complete")

    def _promote_to_submitter(self, serialize=True):
        if self.has_submitter():
            return False

        self._config.submitter = self._hostname
        if serialize:
            self._serialize("promote_to_submitter")

        return True

    def _serialize(self, reason):
        current = self._get_config_version()
        if self._config.version != current:
            raise ConfigVersionMismatch(
                f"expected={current} actual={self._config.version} {reason}"
            )

        # Check the hash before the version update.
        if hash(self._config.json()) != self._config_hash:
            self._config.version += 1
            self._serialize_config_version()
            text = self._config.json()
            self._config_hash = hash(text)
            self._serialize_file(self._config.json(), self._config_file)
            logger.info(
                "Wrote config version %s reason=%s hostname=%s",
                self._config.version,
                reason,
                self._hostname,
            )

    def _serialize_jobs(self, reason):
        current = self._get_job_status_version()
        if self._job_status.version != current:
            raise JobStatusVersionMismatch(
                f"expected={current} actual={self._job_status.version} {reason}"
            )

        # Check the hash before the version update.
        if hash(self._job_status.json()) != self._config_hash:
            self._job_status.version += 1
            self._serialize_job_status_version()
            text = self._job_status.json()
            self._serialize_file(text, self._job_status_file)
            self._job_status_hash = hash(text)
            logger.info(
                "Wrote job_status version %s reason=%s hostname=%s",
                self._job_status.version,
                reason,
                self._hostname,
            )

    @staticmethod
    def _serialize_file(text, filename):
        backup = None
        if os.path.exists(filename):
            backup = filename + ".bk"
            os.rename(filename, backup)
        with open(filename, "w") as f_out:
            f_out.write(text + "\n")
        if backup:
            os.remove(backup)

    def _serialize_config_version(self):
        with open(self._config_version_file, "w") as f_out:
            f_out.write(str(self._config.version) + "\n")

    def _serialize_job_status_version(self):
        with open(self._job_status_version_file, "w") as f_out:
            f_out.write(str(self._job_status.version) + "\n")

    def _update_job_status(
        self,
        submitted_jobs,
        blocked_jobs,
        canceled_jobs,
        completed_job_names,
        hpc_job_ids,
        batch_index,
    ):
        self._job_status.hpc_job_ids = hpc_job_ids
        self._job_status.batch_index = batch_index
        status_lookup = {x.name: x for x in self._job_status.jobs}

        processed = set()
        for job in submitted_jobs:
            assert status_lookup[job.name].state != JobState.SUBMITTED, job.name
            status_lookup[job.name].state = JobState.SUBMITTED
            processed.add(job.name)
            self._config.submitted_jobs += 1

        for job in blocked_jobs:
            old = status_lookup[job.name]
            assert old.state == JobState.NOT_SUBMITTED, f"name={job.name} state={old.state}"
            old.blocked_by = job.blocked_by
            processed.add(job.name)

        for job in canceled_jobs:
            self._config.submitted_jobs += 1

        for name in completed_job_names:
            assert name not in processed, name
            status_lookup[name].state = JobState.DONE
            self._config.completed_jobs += 1

        for job in self.iter_jobs():
            if job.blocked_by and job.state in (JobState.SUBMITTED, JobState.DONE):
                job.blocked_by.clear()

        self._serialize("update_job_status")
        self._serialize_jobs("update_job_status")

        not_submitted = self._config.num_jobs - self._config.submitted_jobs
        logger.info(
            "Updated job status submitted=%s not_submitted=%s completed=%s hostname=%s",
            self._config.submitted_jobs,
            not_submitted,
            self._config.completed_jobs,
            self._hostname,
        )


class ConfigVersionMismatch(Exception):
    """Raised when user tries to perform an update with an old version."""


class JobStatusVersionMismatch(Exception):
    """Raised when user tries to perform an update with an old version."""
