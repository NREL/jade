
import enum
import logging
import os
import socket
from typing import List, Optional, Set, Union

from filelock import SoftFileLock, Timeout

from jade.common import CONFIG_FILE
from jade.exceptions import InvalidConfiguration
from jade.models import ClusterConfig, Job, JobState, JobStatuses
from jade.utils.utils import load_data, dump_data


logger = logging.getLogger(__name__)


LOCK_TIMEOUT = 30


class Cluster:
    """Represents the state of the nodes running jobs."""

    CLUSTER_CONFIG_FILE = "cluster_config.json"
    JOB_STATUS_FILE = "job_status.json"
    LOCK_FILE = "cluster_config.json.lock"

    def __init__(self, config, job_statuses=None, lock_timeout=LOCK_TIMEOUT):
        """Internal constructor. Use create() or deserialize()."""
        self._config = config
        self._lock_file = self.LOCK_FILE
        self._timeout = lock_timeout
        self._job_statuses = job_statuses

    @classmethod
    def create(cls, path, submitter_options, jade_config):
        """Create a new instance of a Cluster. Promotes itself to submitter.

        Parameters
        ----------
        path : str
            Base directory for a JADE submission
        submitter_options : SubmitterOptions

        """
        config = ClusterConfig(
            path = path,
            num_jobs=jade_config.get_num_jobs(),
            submitter=socket.gethostname(),
            submitter_options=submitter_options,
        )
        job_statuses = JobStatuses(
            jobs=[
                Job(
                    name=x.name,
                    blocked_by=x.get_blocking_jobs(),
                    state=JobState.NOT_SUBMITTED,
                )
                for x in jade_config.iter_jobs()
            ],
            hpc_job_ids=[],
        )
        cluster = cls(config, job_statuses=job_statuses)
        cluster.serialize()
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
        """Return True if all jobs are complete

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
        return self._config.submitter == socket.gethostname()

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
            "num_jobs": self._config.num_jobs,
            "completed_jobs": self._config.completed_jobs,
            "not_submitted_jobs": not_submitted,
        }

        if include_jobs:
            summary["job_status"] = self._job_statuses.dict()

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
        assert self._job_statuses is not None
        for job in self._job_statuses.jobs:
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
        assert self._job_statuses is not None
        for job_id in self._job_statuses.hpc_job_ids:
            yield job_id

    @property
    def job_statuses(self):
        """Return the JobStatuses"""
        return self._job_statuses

    def is_complete(self):
        """Return True if the submission is complete."""
        return self._config.is_complete

    def mark_complete(self):
        """Mark the submission as being complete."""
        return self._do_action_under_lock(self._mark_complete)

    def promote_to_submitter(self, serialize=True):
        """Promote the current system to submitter.

        Returns
        -------
        bool
            Returns True if promotion was successful
        """
        return self._do_action_under_lock(self._promote_to_submitter, serialize=serialize)

    def serialize(self):
        """Serialize the config to a file."""
        self._do_action_under_lock(self._serialize)
        logger.info("Serialized config to file")

    def serialize_jobs(self):
        """Serialize the job status to a file."""
        self._do_action_under_lock(self._serialize_jobs)
        logger.info("Serialized config to file")

    def update_job_status(
        self,
        submitted_jobs,
        blocked_jobs,
        completed_job_names,
        hpc_job_ids,
        batch_index,
    ):
        """Update the job status in the config file.

        Parameters
        ----------
        submitted_jobs : list
            list of JobParametersInterface
        blocked_jobs : list
            list of JobParametersInterface
        completed_job_names : set
            set of str
        hpc_job_ids : list
            list of str of newly submitted job IDs
        batch_index : int
            next batch index

        """
        self._do_action_under_lock(
            self._update_job_status, submitted_jobs, blocked_jobs,
            completed_job_names, hpc_job_ids, batch_index
        )

    def _are_all_jobs_complete(self):
        return self._config.completed_jobs == self._config.num_jobs

    def _demote_from_submitter(self, serialize=True):
        assert self.am_i_submitter(), self._config.submitter
        self._config.submitter = None
        if serialize:
            self._serialize()

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
        self._job_statuses = JobStatuses(**data)

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
                "Failed to acquire file lock %s within %s seconds",
                lock_file, timeout
            )
            raise

        try:
            return func(*args, **kwargs)
        finally:
            lock.release()

    def _mark_complete(self):
        assert not self._config.is_complete
        self._config.is_complete = True
        self._serialize()

    def _promote_to_submitter(self, serialize=True):
        if self.has_submitter():
            return False

        self._config.submitter = socket.gethostname()
        if serialize:
            self._serialize()

        return True

    def _serialize(self):
        with open(self.get_config_file(self._config.path), "w") as f_out:
            f_out.write(self._config.json())

        if self._job_statuses is not None:
            self._serialize_jobs()

    def _serialize_jobs(self):
        with open(self.get_job_status_file(self._config.path), "w") as f_out:
            f_out.write(self._job_statuses.json())

    def _update_job_status(
        self,
        submitted_jobs,
        blocked_jobs,
        completed_job_names,
        hpc_job_ids,
        batch_index
    ):
        self._deserialize_jobs()
        self._job_statuses.hpc_job_ids = hpc_job_ids
        self._job_statuses.batch_index = batch_index
        status_lookup = {x.name: x for x in self._job_statuses.jobs}

        processed = set()
        for job in submitted_jobs:
            status_lookup[job.name].state = JobState.SUBMITTED
            processed.add(job.name)
            self._config.submitted_jobs += 1

        for job in blocked_jobs:
            old = status_lookup[job.name]
            assert old.state == JobState.NOT_SUBMITTED, f"name={job.name} state={old.state}"
            old.blocked_by = job.blocked_by
            processed.add(job.name)

        for name in completed_job_names:
            assert name not in processed, name
            status_lookup[name].state = JobState.DONE
            self._config.completed_jobs += 1

        for job in self.iter_jobs():
            if job.blocked_by and job.state in \
                    (JobState.SUBMITTED, JobState.DONE):
                job.blocked_by.clear()

        self._serialize()

        not_submitted = self._config.num_jobs - self._config.submitted_jobs
        logger.info("Updated job status submitted=%s not_submitted=%s completed=%s",
            self._config.submitted_jobs, not_submitted, self._config.completed_jobs)
