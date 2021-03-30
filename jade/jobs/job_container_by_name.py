"""Implements a container for jobs represented by a key."""

from collections import OrderedDict
import logging

from jade.exceptions import InvalidParameter
from jade.jobs.job_container_interface import JobContainerInterface
from jade.utils.utils import check_filename


logger = logging.getLogger(__name__)


class JobContainerByName(JobContainerInterface):
    """Stores jobs by name, which must be unique."""
    def __init__(self):
        # name: JobParametersInterface
        self._jobs = OrderedDict()

    def add_job(self, job):
        self._jobs[job.name] = job
        logger.debug("Added job %s", job.name)

    def clear(self):
        self._jobs.clear()
        logger.debug("Cleared all jobs.")

    def remove_job(self, job):
        self._jobs.pop(job.name)
        logger.info("Removed job %s", job.name)

    def iter_jobs(self):
        for job in self._jobs.values():
            yield job

    def get_job(self, name):
        return self._jobs[name]

    def get_jobs(self, sort_by_name=False):
        if sort_by_name:
            names = list(self._jobs.keys())
            names.sort()
            return [self._jobs[x] for x in names]

        return list(self.iter_jobs())

    def get_num_jobs(self):
        return len(self._jobs)
