"""
Unit tests for JobQueue class
"""
import mock
import time
from jade.jobs.job_queue import JobQueue


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
