"""Implements a container for jobs represented by a key."""

import logging

from jade.exceptions import InvalidParameter
from jade.jobs.job_container_interface import JobContainerInterface
from jade.utils.utils import check_filename


logger = logging.getLogger(__name__)


class JobContainerByKey(JobContainerInterface):
    """Stores jobs by key."""
    def __init__(self):
        self._jobs = {}  # functools.namedtuple: JobParametersInterface

    @staticmethod
    def _get_key(job=None, key=None):
        if key is None and job is None:
            raise InvalidParameter(f"either key or job must be passed")
        if key is not None and job is not None:
            raise InvalidParameter("only one of key and job can be "
                                   "passed")
        if key is None:
            key = job.name

        return key

    def add_job(self, job, key=None):
        if key is None:
            key = job.name

        if key in self._jobs:
            raise InvalidParameter(f"key={key} is already stored")

        check_filename(key)

        self._jobs[key] = job
        logger.debug("Added job %s", key)

    def clear(self):
        self._jobs.clear()
        logger.debug("Cleared all jobs.")

    def remove_job(self, job=None, key=None):
        key = self._get_key(job=job, key=key)
        self._jobs.pop(key)
        logger.info("Removed job with key=%s", key)

    def iter_jobs(self):
        for job in self._jobs.values():
            yield job

    def get_job(self, name):
        for job in self.iter_jobs():
            if job.name == name:
                return job

        raise InvalidParameter(f"job {name} not found")

    def get_jobs(self, sort_by_key=False):
        if sort_by_key:
            jobs = [val for _, val in sorted(self._jobs.items())]
        else:
            jobs = list(self._jobs.values())

        return jobs

    def get_num_jobs(self):
        return len(self._jobs)
