
import os

import pytest

from jade.jobs.job_submitter import JobSubmitter


TEST_DATA_DIR = "tests/data/example_output"


def test_jobs_submitter__find_error_log_messages():
    events = list(JobSubmitter.find_error_log_messages(TEST_DATA_DIR))
    assert len(events) == 4
    assert events[0].data["error"] == "Traceback"
    assert events[0].data["filename"] == f"{TEST_DATA_DIR}/job_output_2741821.e"
    assert events[0].data["line_number"] == 2
    assert events[0].data["text"] == "Traceback (most recent call last):"
    assert events[1].data["error"] == "DUE TO TIME LIMIT"
    assert events[1].data["line_number"] == 43
    assert events[2].data["error"] == "slurmstepd"
    assert events[2].data["line_number"] == 44
    assert events[3].data["error"] == "srun"
    assert events[3].data["line_number"] == 45


@pytest.fixture
def cleanup():
    def delete_files():
        for filename in ("errors.txt", "results.txt", "stats.txt"):
            path = os.path.join(TEST_DATA_DIR, filename)
            if os.path.exists(path):
                os.remove(path)
    delete_files()
    yield
    delete_files()


def test_jobs_submitter__generate_reports(cleanup):
    ret = JobSubmitter.generate_reports(TEST_DATA_DIR)
    assert ret == 0
    for filename in ("errors.txt", "results.txt", "stats.txt"):
        path = os.path.join(TEST_DATA_DIR, filename)
        assert os.path.exists(path)
