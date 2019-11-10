"""Defines class for managing a job queue."""

from collections import OrderedDict
import logging
import time


logger = logging.getLogger(__name__)


DEFAULT_POLL_INTERVAL = 1


class JobQueue:
    """Submits jobs for execution in parallel."""

    def __init__(self, max_queue_depth, poll_interval=DEFAULT_POLL_INTERVAL):
        """
        Parameters
        ----------
        max_queue_depth : int
            Maximum number of sub-processes to maintain
        poll_interval : int
            Seconds to sleep in between completion checks.

        """
        self._queue_depth = max_queue_depth
        self._poll_interval = poll_interval

    def run(self, jobs):
        """
        Run job queue.

        Parameters
        ----------
        jobs : list
            List of DispatchableJobInterface objects to run.

        """
        outstanding_jobs = OrderedDict()

        num_jobs = 0
        num_completed = 0
        for job in jobs:
            job.run()
            num_jobs += 1
            outstanding_jobs[job.name()] = job

            while True:
                # Pops complete jobs.
                num_completed += self._check_completions(outstanding_jobs)

                if len(outstanding_jobs) >= self._queue_depth:
                    time.sleep(self._poll_interval)
                else:
                    break

        while outstanding_jobs:
            num_completed += self._check_completions(outstanding_jobs)
            time.sleep(self._poll_interval)

        assert num_jobs == num_completed, f"{num_jobs} {num_completed}"

    @staticmethod
    def _check_completions(outstanding_jobs):
        completed_jobs = []
        for name, job in outstanding_jobs.items():
            if job.is_complete():
                completed_jobs.append(job.name())

        for name in completed_jobs:
            outstanding_jobs.pop(name)

        return len(completed_jobs)

    @classmethod
    def run_jobs(cls, jobs, max_queue_depth,
                 poll_interval=DEFAULT_POLL_INTERVAL):
        """
        Run job queue.

        Parameters
        ----------
        jobs : list
            List of cli calls to submit to sub-processes
        max_queue_depth : int
            Maximum number of sub-processes to maintain
        poll_interval : int
            Seconds to sleep in between completion checks.

        """
        queue = cls(max_queue_depth, poll_interval=poll_interval)
        queue.run(jobs)
