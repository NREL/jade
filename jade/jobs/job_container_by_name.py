"""Implements a container for jobs represented by a key."""

import logging

from jade.exceptions import InvalidParameter
from jade.jobs.job_container_interface import JobContainerInterface


logger = logging.getLogger(__name__)


class JobContainerByName(JobContainerInterface):
    """Stores jobs by name, which must be unique."""

    def __init__(self):
        # name: JobParametersInterface
        self._jobs = {}

    def __iter__(self):
        for job in self._jobs.values():
            yield job

    def __len__(self):
        return len(self._jobs)

    def add_job(self, job):
        self._jobs[job.name] = job
        logger.debug("Added job %s", job.name)

    def clear(self):
        self._jobs.clear()
        logger.debug("Cleared all jobs.")

    def remove_job(self, job):
        self._jobs.pop(job.name)
        logger.info("Removed job %s", job.name)

    def get_job(self, name):
        job = self._jobs.get(name)
        if job is None:
            raise InvalidParameter(f"job={name} is not stored")
        return self._jobs[name]

    def get_jobs(self, sort=False):
        if sort:
            names = list(self._jobs.keys())
            names.sort()
            return [self._jobs[x] for x in names]

        return list(self)
