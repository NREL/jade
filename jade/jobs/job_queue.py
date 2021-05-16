"""Defines class for managing a job queue."""

from collections import OrderedDict
import logging
import os
import time

from jade.models.submitter_params import DEFAULTS

logger = logging.getLogger(__name__)


DEFAULT_POLL_INTERVAL = 1


class JobQueue:
    """Submits jobs for execution in parallel.

    There are two ways to use this class:

    1. Build a list of jobs and pass that list to :meth:`JobQueue.run_jobs`.
       It will run to completion.
    2. Call :meth:`JobQueue.submit` as jobs become ready to run. JobQueue will
       either run it immediately or queue it if too many commands are
       oustanding.  In this mode it is up to the caller to call
       :meth:`JobQueue.process_queue` periodically. That will look for job
       completions pull new jobs off the queue. JobQueue does not start a
       background thread to do this automatically.

    """

    def __init__(
        self,
        max_queue_depth,
        existing_jobs=None,
        poll_interval=DEFAULT_POLL_INTERVAL,
        monitor_func=None,
        monitor_interval=DEFAULTS["resource_monitor_interval"],
    ):
        """
        Parameters
        ----------
        max_queue_depth : int
            Maximum number of sub-processes to maintain
        poll_interval : int
            Seconds to sleep in between completion checks.
        monitor_interval : int
            Seconds to sleep in between resource monitor cycles.
            Can be overridden with environment variable JADE_MONITOR_INTERVAL.

        """
        self._queue_depth = max_queue_depth
        self._poll_interval = poll_interval
        self._outstanding_jobs = OrderedDict()
        self._queued_jobs = []
        self._num_jobs = 0
        self._num_completed = 0
        self._monitor_func = monitor_func
        self._last_monitor_time = None

        if existing_jobs is not None:
            for job in existing_jobs:
                self._outstanding_jobs[job.name] = job

        interval = os.environ.get("JADE_MONITOR_INTERVAL")
        if interval:
            self._monitor_interval = int(interval)
        else:
            self._monitor_interval = monitor_interval

        logger.debug(
            "queue_depth=%s poll_interval=%s monitor_interval=%s",
            self._queue_depth,
            self._poll_interval,
            self._monitor_interval,
        )

    def _check_completions(self):
        logger.debug("check for completions")
        failed_jobs = set()
        # If jobs fail and are configured to cancel blocked jobs, we may need to run this
        # loop many times to cancel the entire chain.
        need_to_rerun = True
        while need_to_rerun:
            need_to_rerun = False
            completed_jobs = []
            for name, job in self._outstanding_jobs.items():
                if job.is_complete():
                    completed_jobs.append(name)
                    if job.return_code != 0:
                        failed_jobs.add(job.name)

            self._num_completed += len(completed_jobs)
            logger.debug("found num_completed=%s", len(completed_jobs))
            for name in completed_jobs:
                self._outstanding_jobs.pop(name)
                logger.debug("Completed a job %s", name)

                canceled_indices = []
                for i, job in enumerate(self._queued_jobs):
                    blocking_jobs = job.get_blocking_jobs()
                    if blocking_jobs:
                        if job.cancel_on_blocking_job_failure and blocking_jobs.intersection(
                            failed_jobs
                        ):
                            job.set_blocking_jobs(set())
                            job.cancel()
                            canceled_indices.append(i)
                            self._num_jobs += 1
                            self._outstanding_jobs[job.name] = job
                            need_to_rerun = True
                        elif name in blocking_jobs:
                            logger.debug("Remove %s from job=%s blocked list", name, job.name)
                            job.remove_blocking_job(name)

                for index in reversed(canceled_indices):
                    self._queued_jobs.pop(index)

    def _run_job(self, job):
        logger.debug("Run job %s", job.name)
        job.run()
        self._num_jobs += 1
        self._outstanding_jobs[job.name] = job

    def is_full(self):
        """Return True if the max number of jobs is outstanding.

        Returns
        -------
        bool

        """
        return len(self._outstanding_jobs) >= self._queue_depth

    def _handle_monitor_func(self, force=False):
        if self._monitor_func is None:
            return

        needs_monitor = False
        cur_time = time.time()
        if force:
            needs_monitor = True
        elif self._last_monitor_time is None:
            needs_monitor = True
        elif cur_time - self._last_monitor_time > self._monitor_interval:
            needs_monitor = True

        if needs_monitor:
            logger.debug("Run monitor function")
            self._monitor_func()
            self._last_monitor_time = cur_time

    @property
    def outstanding_jobs(self):
        """Return the outstanding jobs.

        Returns
        -------
        dict_values

        """
        return self._outstanding_jobs.values()

    def process_queue(self):
        """Process completions and submit new jobs if the queue is not full."""
        self._handle_monitor_func()
        self._check_completions()
        if not self._queued_jobs:
            logger.debug("queue is empty; nothing to do")
            return

        jobs_to_pop = []
        available_jobs = self._queue_depth - len(self._outstanding_jobs)
        if available_jobs == 0:
            logger.debug("queue is full")
            return

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

        logger.debug(
            "Started %s jobs in process_queue; num_blocked=%s", len(jobs_to_pop), num_blocked
        )

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
        The caller should call :meth:`JobQueue.process_queue` periodically to check
        for completions and start new jobs.

        Parameters
        ----------
        job : AsyncJobInterface

        """
        if self.is_full():
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

        assert self._num_completed == self._num_jobs, f"{self._num_completed} {self._num_jobs}"

        self._handle_monitor_func(force=True)

    @classmethod
    def run_jobs(
        cls,
        jobs,
        max_queue_depth,
        poll_interval=DEFAULT_POLL_INTERVAL,
        monitor_func=None,
        monitor_interval=DEFAULTS["resource_monitor_interval"],
    ):
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
        monitor_func : callable
            Optionally a function to call each poll_interval, such as for
            resource monitoring.
        monitor_interval : int
            Interval in seconds on which to run monitor_func.

        """
        queue = cls(
            max_queue_depth,
            poll_interval=poll_interval,
            monitor_func=monitor_func,
            monitor_interval=monitor_interval,
        )
        queue.run(jobs)
