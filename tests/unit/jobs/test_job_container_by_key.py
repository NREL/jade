"""
Unit tests for JobContainerByKey class
"""
import mock
import pytest
from jade.exceptions import InvalidParameter
from jade.jobs.job_container_by_key import JobContainerByKey


def test_job_conatainer_by_key():
    """Test Job container"""
    job_container = JobContainerByKey()

    # Define jobs
    job1 = mock.MagicMock()
    job1.name = "TestJob1"

    job2 = mock.MagicMock()
    job2.name = "TestJob2"

    # Add job with key=None
    job_container.add_job(job1)
    assert len(job_container) == 1
    assert job1.name in job_container._jobs

    # Add job with key != None
    job_container.add_job(job2, key=job2.name)
    assert len(job_container) == 2
    assert job2.name in job_container._jobs

    # Add job, but job already in container
    with pytest.raises(InvalidParameter) as exc:
        job_container.add_job(job2)
    assert "key=TestJob2 is already stored" in str(exc.value)

    # Get job by name
    test_job = job_container.get_job("TestJob1")
    assert test_job == job1

    with pytest.raises(InvalidParameter) as exc:
        job_container.get_job("OtherJob")

    assert "job OtherJob not found" in str(exc.value)

    # Iter jobs
    jobs = iter(job_container)
    assert next(jobs) in [job1, job2]
    assert next(jobs) in [job1, job2]

    # Remove jobs
    job_container.remove_job(key=job1.name)
    assert len(job_container) == 1
    assert job1.name not in job_container._jobs

    with pytest.raises(InvalidParameter) as exc:
        job_container.remove_job(job=None, key=None)
    assert "either key or job must be passed" in str(exc.value)

    with pytest.raises(InvalidParameter) as exc:
        job_container.remove_job(job=job2, key="X")
    assert "only one of key and job can be passed" in str(exc.value)

    # Clear jobs
    job_container.clear()
    assert len(job_container) == 0

    # Job with illegal name
    illegal_job = mock.MagicMock()
    illegal_job.name = "Illegal/Job"
    with pytest.raises(InvalidParameter) as exc:
        job_container.add_job(illegal_job)
