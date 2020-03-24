"""Defines class for managing a job queue."""

from collections import OrderedDict
import logging
import time


logger = logging.getLogger(__name__)


DEFAULT_POLL_INTERVAL = 1


class JobQueue:
    """Submits jobs for execution in parallel.

    There are two ways to use this class:
    1. Build a list of jobs and pass that list to `JobQueue.run_jobs`.
       It will run to completion.
    2. Call `JobQueue.submit` as jobs become ready to run. JobQueue will either
       run it immediately or queue it if too many commands are oustanding.
       In this mode it is up to the caller to call `JobQueue.process_queue`
       periodically. That will look for job completions pull new jobs off the
       queue. JobQueue does not start a background thread to do this
       automatically.

    """

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
        self._outstanding_jobs = OrderedDict()
        self._queued_jobs = []
        self._num_jobs = 0
        self._num_completed = 0

        logger.debug("queue_depth=%s poll_interval=%s", self._queue_depth,
                     self._poll_interval)

    def _check_completions(self):
        logger.debug("check for completions")
        completed_jobs = []
        for name, job in self._outstanding_jobs.items():
            if job.is_complete():
                completed_jobs.append(name)

        self._num_completed += len(completed_jobs)
        logger.debug("found num_completed=%s", len(completed_jobs))
        for name in completed_jobs:
            self._outstanding_jobs.pop(name)
            logger.debug("Completed a job %s", name)

            for _job in self._queued_jobs:
                if name in _job.get_blocking_jobs():
                    logger.debug("Remove %s from job=%s blocked list",
                                 name, _job.name)
                    _job.remove_blocked_job(name)

    def _run_job(self, job):
        logger.debug("Run job %s", job.name)
        job.run()
        self._num_jobs += 1
        self._outstanding_jobs[job.name] = job

    def process_queue(self):
        """Process completions and submit new jobs if the queue is not full."""
        self._check_completions()
        if not self._queued_jobs:
            logger.debug("queue is empty; nothing to do")
            return

        jobs_to_pop = []
        available_jobs = self._queue_depth - len(self._outstanding_jobs)
        num_blocked = 0
        for i, job in enumerate(self._queued_jobs):
            blocking = job.get_blocking_jobs()
            if blocking:
                logger.debug("job %s is blocked by %s", job.name, blocking)
                num_blocked += 1
                continue

            self._run_job(job)
            jobs_to_pop.append(i)
            if len(jobs_to_pop) >= available_jobs:
                break

        for index in reversed(jobs_to_pop):
            self._queued_jobs.pop(index)

        logger.debug("Started %s jobs in process_queue; num_blocked=%s",
                     len(jobs_to_pop), num_blocked)

    def run(self, jobs):
        """
        Run job queue synchronously. Blocks until all jobs are complete.

        Parameters
        ----------
        jobs : list
            List of AsyncJobInterface objects to run.

        """
        for job in jobs:
            self.submit(job)

        self.wait()

    def submit(self, job):
        """Submit a job to be executed. If the queue is not full then it will
        run the job. Otherwise, it will queue the job. Returns immediately.
        The caller should call `JobQueue.process_queue` periodically to check
        for completions and start new jobs.

        Parameters
        ----------
        job : AsyncJobInterface

        """
        if len(self._outstanding_jobs) >= self._queue_depth:
            logger.debug("queue depth exceeded, queue job %s", job.name)
            self._queued_jobs.append(job)
        elif job.get_blocking_jobs():
            logger.debug("Job is blocked by %s", job.get_blocking_jobs())
            self._queued_jobs.append(job)
        else:
            self._run_job(job)
            logger.debug("Started job %s in submit", job.name)

    def wait(self):
        """Return once all jobs have completed."""
        while self._outstanding_jobs or self._queued_jobs:
            self.process_queue()
            time.sleep(self._poll_interval)

        assert self._num_completed == self._num_jobs, \
            f"{self._num_completed} {self._num_jobs}"

    @classmethod
    def run_jobs(cls, jobs, max_queue_depth,
                 poll_interval=DEFAULT_POLL_INTERVAL):
        """
        Run job queue synchronously. Blocks until all jobs are complete.

        Parameters
        ----------
        jobs : list
            List of AsyncJobInterface objects
        max_queue_depth : int
            Maximum number of parallel jobs to maintain
        poll_interval : int
            Seconds to sleep in between completion checks.

        """
        queue = cls(max_queue_depth, poll_interval=poll_interval)
        queue.run(jobs)
