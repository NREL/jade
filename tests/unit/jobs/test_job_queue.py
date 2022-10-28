"""
Unit tests for JobQueue class
"""

import logging
import mock
import time

import pytest

from jade.enums import Status
from jade.jobs.async_job_interface import AsyncJobInterface
from jade.jobs.job_queue import JobQueue


class FakeJob(AsyncJobInterface):
    def __init__(self, name, duration, blocking_jobs=None):
        self._name = name
        self._duration = duration
        self._blocking_jobs = set() if blocking_jobs is None else blocking_jobs
        self.start_time = None
        self.end_time = None
        self._is_complete = False

    def cancel(self):
        self._is_complete = True

    @property
    def cancel_on_blocking_job_failure(self):
        return False

    def get_id(self):
        return 0

    def is_complete(self):
        if self._is_complete:
            return True
        return time.time() > self.end_time

    @property
    def name(self):
        return self._name

    @property
    def return_code(self):
        assert self.is_complete()
        return 0

    def run(self):
        self.start_time = time.time()
        self.end_time = self.start_time + self._duration
        return Status.GOOD

    def get_blocking_jobs(self):
        return self._blocking_jobs

    def remove_blocking_job(self, name):
        self._blocking_jobs.remove(name)

    def set_blocking_jobs(self, jobs):
        assert False


def test_job_queue__is_full():
    duration = 10
    jobs = [FakeJob(str(i), duration) for i in range(4)]
    queue = JobQueue(2, poll_interval=1)
    assert not queue.is_full()
    for job in jobs:
        queue.submit(job)
    assert queue.is_full()


def test_job_queue__run_jobs_no_ordering():
    duration = 0.1
    jobs = [FakeJob(str(i), duration) for i in range(10)]
    JobQueue.run_jobs(jobs, 5, poll_interval=0.1)
    for job in jobs:
        assert job.is_complete()


def test_job_queue__run_jobs_ordering():
    duration = 0.1
    jobs = {}
    for i in range(1, 11):
        name = str(i)
        if i == 1:
            job = FakeJob(name, duration, blocking_jobs=set(["10"]))
        elif i == 2:
            job = FakeJob(name, duration, blocking_jobs=set(["1"]))
        elif i == 3:
            job = FakeJob(name, duration, blocking_jobs=set(["4", "5"]))
        else:
            job = FakeJob(name, duration)
        jobs[name] = job
    JobQueue.run_jobs(jobs.values(), 5, poll_interval=0.1)

    for job in jobs.values():
        assert job.is_complete()
        assert not job.get_blocking_jobs()

    assert jobs["1"].start_time > jobs["10"].end_time
    assert jobs["2"].start_time > jobs["1"].end_time
    assert jobs["3"].start_time > jobs["4"].end_time
    assert jobs["3"].start_time > jobs["5"].end_time


def test_job_queue__monitor_func():
    has_run = []

    def monitor(ids=None):
        has_run.append(1)

    duration = 0.1
    jobs = [FakeJob(str(i), duration) for i in range(1)]
    JobQueue.run_jobs(jobs, 5, poll_interval=0.1, monitor_func=monitor)
    assert has_run


def job_run():
    """Job run"""
    time.sleep(0.5)


def test_job_queue():
    """Job queue should run with expection"""

    job1 = mock.MagicMock()
    job1.name = "Job1"

    job2 = mock.MagicMock()
    job2.name = "Job2"

    job3 = mock.MagicMock()
    job3.name = "Job3"

    jobs = [job1, job2, job3]

    # TODO: Finish up this test
    # JobQueue.run_jobs(jobs, max_queue_depth=2)
