from pathlib import Path

import pytest

from jade.jobs.job_submitter import JobSubmitter


def test_jobs_submitter__find_error_log_messages(example_output):
    events = list(JobSubmitter.find_error_log_messages(example_output))
    assert len(events) == 4
    assert events[0].data["error"] == "Traceback"
    assert events[0].data["filename"] == f"{example_output}/job_output_2741821.e"
    assert events[0].data["line_number"] == 2
    assert events[0].data["text"] == "Traceback (most recent call last):"
    assert events[1].data["error"] == "DUE TO TIME LIMIT"
    assert events[1].data["line_number"] == 43
    assert events[2].data["error"] == "slurmstepd"
    assert events[2].data["line_number"] == 44
    assert events[3].data["error"] == "srun"
    assert events[3].data["line_number"] == 45


@pytest.fixture
def cleanup(example_output):
    def delete_files():
        for filename in ("errors.txt", "results.txt", "stats.txt"):
            path = Path(example_output) / filename
            if path.exists():
                path.unlink()

    delete_files()
    yield
    delete_files()


def test_jobs_submitter__generate_reports(example_output, cleanup):
    ret = JobSubmitter.generate_reports(example_output, True)
    assert ret == 0
    for filename in ("errors.txt", "results.txt", "stats.txt"):
        path = Path(example_output) / filename
        assert path.exists()
