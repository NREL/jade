"""
Unit tests for auto-regression execution class methods and properties.
"""

import os
import shutil
import tempfile

import pytest

import pandas as pd
from mock import MagicMock, patch
from jade.extensions.demo.autoregression_execution import autoregression_analysis
from jade.extensions.demo.autoregression_execution import AutoRegressionExecution


# TODO: the statsmodel function calls are deprecated and need to be updated.
@pytest.mark.skip
def test_autoregression_analysis(test_data_dir):
    """Should return csv result and png plot"""
    country = "Mock Country"
    data = os.path.join(test_data_dir, "demo", "mock_country.csv")
    output = os.path.join(tempfile.gettempdir(), "jade-unit-test-dir")
    os.makedirs(output, exist_ok=True)

    result_file, plot_file = autoregression_analysis(country, data, output)
    assert result_file == os.path.join(
        output,
        "result.csv",
    )
    assert plot_file == os.path.join(
        output,
        "result.png",
    )

    df = pd.read_csv(result_file)
    assert "pred_gdp" in df.columns

    if os.path.exists(output):
        shutil.rmtree(output)


def test_results_directory():
    """Should returen the output directory"""
    job = MagicMock()
    output = os.path.join(tempfile.gettempdir(), "jade-unit-test-output")
    are = AutoRegressionExecution(job=job, output=output)

    assert are.results_directory == output
    assert os.path.exists(are.results_directory)

    shutil.rmtree(output)


def test_create():
    """Should return a instance of AutoRegressionExecution"""
    job = MagicMock()
    job.name = "Job1"
    output = os.path.join(tempfile.gettempdir(), "jade-unit-test-output")
    are = AutoRegressionExecution.create(None, job, output)
    assert isinstance(are, AutoRegressionExecution)


def test_generate_command():
    """Should return a command line string"""
    job = MagicMock()
    job.name = "Job1"
    output = os.path.join(tempfile.gettempdir(), "o")
    config_file = os.path.join(tempfile.gettempdir(), "config-file")

    cmd = AutoRegressionExecution.generate_command(job, output, config_file)
    assert (
        cmd == f"jade-internal run demo --name=Job1 --output={output} --config-file={config_file}"
    )


def test_list_results_files():
    """Should return a list of files in output directory"""
    job = MagicMock()
    job.name = "Job1"
    output = os.path.join(tempfile.gettempdir(), "jade-unit-test-output")
    job_dir = os.path.join(output, job.name)

    result_file = os.path.join(job_dir, "result.csv")
    with open(result_file, "w") as f:
        f.write("data")

    are = AutoRegressionExecution(job, output)
    results = are.list_results_files()
    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0] == job_dir

    shutil.rmtree(output)


def run_autoregression_analysis(*args, **kwargs):
    """Side effect of mock regression analysis"""
    return "result.csv", "result.png"


@patch("jade.extensions.demo.autoregression_execution.autoregression_analysis")
def test_run(mock_autoregression_analysis):
    """Should call the autoregerssion_analysis method defined outside of class"""
    job = MagicMock()
    job.name = "united_states"
    job.country = "united_states"
    job.data = "data.csv"
    output = os.path.join(tempfile.gettempdir(), "jade-unit-test-output")

    mock_autoregression_analysis.side_effect = run_autoregression_analysis

    are = AutoRegressionExecution(job, output)
    are.run()
    mock_autoregression_analysis.assert_called_once()
    mock_autoregression_analysis.assert_called_with(
        country="united_states",
        data="data.csv",
        output=os.path.join(output, job.name),
    )
    shutil.rmtree(output)
