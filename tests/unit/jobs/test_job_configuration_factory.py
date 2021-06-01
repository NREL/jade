"""
Unit tests for JobQueue class
"""
import mock
import os
import pytest

from jade.exceptions import InvalidParameter
from jade.jobs.job_configuration_factory import (
    create_config_from_file,
    create_config_from_previous_run,
)
from jade.result import Result, ResultsSummary, serialize_results


@pytest.fixture
def jade_data():
    """Fixture of serialized jade result"""
    return {
        "base_directory": "/jade/results/base/directory/",
        "missing_jobs": [],
        "results": [
            {
                "name": "australia",
                "return_code": 1,
                "status": "finished",
                "exec_time_s": 10,
                "completion_time": 15555555555,
            },
            {
                "name": "brazil",
                "return_code": 0,
                "status": "finished",
                "exec_time_s": 20,
                "completion_time": 15555555555,
            },
            {
                "name": "united_states",
                "return_code": 0,
                "status": "finished",
                "exec_time_s": 30,
                "completion_time": 15555555555,
            },
        ],
        "jade_version": 0.1,
        "timestamp": "2019-09-02 15:00:00",
    }


@pytest.fixture
def results_summary(jade_data):
    """Fixture of ResultsSummary instance"""
    ResultsSummary._parse = mock.MagicMock(return_value=jade_data)


@pytest.fixture
def incomplete_results(jade_data):
    """Fixture of ResultsSummary instance"""
    jade_data["results"] = jade_data["results"][:2]
    ResultsSummary._parse = mock.MagicMock(return_value=jade_data)


@pytest.fixture
def test_data_dir(test_data_dir):
    """The path to the directory that contains the fixture data"""
    return os.path.join(test_data_dir, "demo")


@pytest.fixture
def config_file(test_data_dir):
    return os.path.join(test_data_dir, "test-config.json")


@pytest.fixture
def output_dir(test_data_dir):
    return os.path.join(test_data_dir, "output")


def test_create_config_from_file(config_file):
    """Create should successfully return config"""
    config = create_config_from_file(config_file)
    assert len(config.list_jobs()) == 3


def test_create_config_from_file_missing_file(config_file):
    """Create should throw FileNotFoundError"""
    with pytest.raises(FileNotFoundError):
        create_config_from_file("a" + config_file)


def test_create_config_from_previous_run_successful_results(
    config_file, output_dir, results_summary
):
    """Create should return config with 2 jobs"""
    successful_config = create_config_from_previous_run(config_file, output_dir)
    assert len(successful_config.list_jobs()) == 2
    for job in successful_config.list_jobs():
        assert job.name in ["brazil", "united_states"]


def test_create_config_from_previous_run_failed_results(config_file, output_dir, results_summary):
    """Create should return config with 1 job"""
    failed_config = create_config_from_previous_run(config_file, output_dir, "failed")
    assert len(failed_config.list_jobs()) == 1
    for job in failed_config.list_jobs():
        assert job.name in ["australia"]


def test_create_config_from_previous_run_missing_results(
    config_file, output_dir, incomplete_results
):
    """Create should return config with 1 job"""
    missing_config = create_config_from_previous_run(config_file, output_dir, "missing")
    assert len(missing_config.list_jobs()) == 1
    for job in missing_config.list_jobs():
        assert job.name in ["united_states"]


def test_create_config_from_previous_run_invalid_type_results(
    config_file, output_dir, results_summary
):
    """Create should throw InvalidParameter"""
    with pytest.raises(InvalidParameter):
        create_config_from_previous_run(config_file, output_dir, "invalid_type")
