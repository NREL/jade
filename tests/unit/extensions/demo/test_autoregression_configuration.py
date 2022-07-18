"""
Unit tests for auto-regression configuration methods and properties.
"""
import os
import tempfile
import shutil

import pytest
from mock import MagicMock, patch

from jade.exceptions import InvalidParameter
from jade.extensions.demo.autoregression_configuration import AutoRegressionConfiguration
from jade.extensions.demo.autoregression_execution import AutoRegressionExecution
from jade.extensions.demo.autoregression_parameters import AutoRegressionParameters
from jade.jobs.job_configuration import JobConfiguration


def test_init():
    """Should return expected attributes after initialization"""
    job_inputs = MagicMock()
    arc = AutoRegressionConfiguration(job_inputs=job_inputs)


def test_create_from_result():
    """Should return None as not implemented"""
    job_inputs = MagicMock()
    arc = AutoRegressionConfiguration(
        job_inputs=job_inputs,
    )
    assert arc.create_from_result(MagicMock(), "output") is None


def test_autogression_configuration__add_job():
    """Should add job to container"""
    arc = AutoRegressionConfiguration()
    job1 = MagicMock()
    job1.name = "test"
    arc.add_job(job1)
    job2 = MagicMock()
    job2.name = "test2"
    arc.add_job(job2)
    assert arc.get_num_jobs() == 2


def test_clear():
    """Should clear jobs container"""
    arc = AutoRegressionConfiguration()
    job1 = MagicMock()
    job1.name = "test"
    arc.add_job(job1)
    job2 = MagicMock()
    job2.name = "test2"
    arc.add_job(job2)
    assert arc.get_num_jobs() == 2

    arc.clear()
    assert arc.get_num_jobs() == 0


def test_dump():
    """Should convert the configuration to json format"""
    filename = os.path.join(tempfile.gettempdir(), "jade-unit-test-arc.json")
    arc = AutoRegressionConfiguration()
    arc.dump(filename=filename)
    assert os.path.exists(filename)
    os.remove(filename)


def test_dumps():
    """Should call json to perform dumps"""
    arc = AutoRegressionConfiguration()
    string = arc.dumps()
    assert "demo" in string
    assert "AutoRegressionConfiguration" in string
    assert "jobs = []" in string


def test_deserialize():
    """Should create an instance from saved configuration file"""
    data = {
        "jobs_directory": os.path.join(
            tempfile.gettempdir(),
            "my_jobs_base_dir",
        ),
        "format_version": JobConfiguration.FORMAT_VERSION,
    }
    arc = AutoRegressionConfiguration.deserialize(data)
    assert isinstance(arc, AutoRegressionConfiguration)
    assert arc._jobs_directory == os.path.join(
        tempfile.gettempdir(),
        "my_jobs_base_dir",
    )


def test_get_job():
    """Should return the job expected"""
    arc = AutoRegressionConfiguration()
    job1 = MagicMock()
    job1.name = "Job1"
    arc.add_job(job1)

    job2 = MagicMock()
    job2.name = "Job2"
    arc.add_job(job2)

    job = arc.get_job("Job1")
    assert job == job1


def test_iter_jobs():
    """Should iterate jobs in container"""
    arc = AutoRegressionConfiguration()
    job1 = MagicMock()
    job1.name = "Job1"
    arc.add_job(job1)

    job2 = MagicMock()
    job2.name = "Job2"
    arc.add_job(job2)

    jobs = arc.iter_jobs()
    assert next(jobs) in [job1, job2]
    assert next(jobs) in [job1, job2]

    with pytest.raises(StopIteration):
        next(jobs)


def test_list_jobs():
    """Should return a list of jobs"""
    arc = AutoRegressionConfiguration()
    job1 = MagicMock()
    job1.name = "Job1"
    arc.add_job(job1)

    job2 = MagicMock()
    job2.name = "Job2"
    arc.add_job(job2)

    jobs = arc.list_jobs()
    assert len(jobs) == 2


def test_reconfigure_jobs():
    """Should reconfigure with a list of jobs"""
    job1 = MagicMock()
    job1.name = "Job1"
    arc = AutoRegressionConfiguration()
    arc.add_job(job1)

    job2 = MagicMock()
    job2.name = "Job2"

    job3 = MagicMock()
    job3.name = "Job3"

    arc.reconfigure_jobs(jobs=[job2, job3])
    assert arc.get_num_jobs() == 2

    with pytest.raises(InvalidParameter):
        arc.get_job("Job1")

    assert arc.get_job("Job2") == job2


def test_remove_job():
    """Should remove job if job exists in container"""
    arc = AutoRegressionConfiguration()
    job1 = MagicMock()
    job1.name = "Job1"
    arc.add_job(job1)

    job2 = MagicMock()
    job2.name = "Job2"
    arc.add_job(job2)

    arc.remove_job(job1)
    assert arc.get_num_jobs() == 1


# @patch("jade.extensions.demo.autoregression_configuration.AutoRegressionExecution")
# def test_run_job(mock_job_execution_class):
#    """Should run job using run method defined in AutoRegression class"""
#    arc = AutoRegressionConfiguration()
#    job1 = MagicMock()
#    job1.name = "Job1"
#    arc.add_job(job1)
#
#    job_execution_instance = MagicMock()
#    job_execution_instance.run.return_value = "results"
#
#    output = os.path.join(tempfile.gettempdir(), "jade-unit-test-output")
#    arc.run_job(job1, output)
#    assert job_execution_instance.run.called


def test_serialize():
    """Should create data for serialization"""
    arc = AutoRegressionConfiguration()
    data = arc.serialize()

    expected = {
        "configuration_class": "AutoRegressionConfiguration",
        "configuration_module": "jade.extensions.demo.autoregression_configuration",
        "format_version": "v0.2.0",
        "jobs_directory": None,
        "jobs": [],
        "user_data": {},
        "submission_groups": [],
        "setup_command": None,
        "teardown_command": None,
        "node_setup_command": None,
        "node_teardown_command": None,
    }
    assert data == expected


def test_serialize_jobs():
    """Should serialize a series of jobs"""
    arc = AutoRegressionConfiguration()
    job1 = AutoRegressionParameters(country="A", data="A.csv")
    arc.add_job(job1)

    job2 = AutoRegressionParameters(country="B", data="B.csv")
    arc.add_job(job2)

    directory = os.path.join(tempfile.gettempdir(), "jade-unit-test-dir")
    os.makedirs(directory, exist_ok=True)
    arc.serialize_jobs(directory)

    serialized_job1 = os.path.join(directory, "a.json")
    assert os.path.exists(serialized_job1)

    serialized_job2 = os.path.join(directory, "b.json")
    assert os.path.exists(serialized_job2)

    shutil.rmtree(directory)


def test_serialize_for_execution():
    """Serialize config data for efficient execution"""
    arc = AutoRegressionConfiguration()
    job1 = AutoRegressionParameters(country="AA", data="AA.csv")
    arc.add_job(job1)

    job2 = AutoRegressionParameters(country="BB", data="BB.csv")
    arc.add_job(job2)

    # Serialize for execution
    scratch_dir = os.path.join(
        tempfile.gettempdir(),
        "jade-unit-test-scratch-dir",
    )
    os.makedirs(scratch_dir, exist_ok=True)
    arc.serialize_for_execution(scratch_dir)

    config_file = os.path.join(scratch_dir, "config.json")
    assert os.path.exists(config_file)

    shutil.rmtree(scratch_dir)


def test_show_results(capsys):
    """Should print jobs to std.out"""
    arc = AutoRegressionConfiguration()
    job1 = AutoRegressionParameters(country="AAA", data="AA.csv")
    arc.add_job(job1)

    arc.show_jobs()

    captured = capsys.readouterr()
    assert "aaa" in captured.out
