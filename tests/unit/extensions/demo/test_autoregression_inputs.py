"""
Units tests for auto-regression inputs class methods and properties
"""
import os
from jade.extensions.demo.autoregression_inputs import AutoRegressionInputs
from jade.extensions.demo.autoregression_parameters import AutoRegressionParameters


def test_base_directory(test_data_dir):
    """Should return a string path"""
    base_directory = os.path.join(test_data_dir, "demo")

    ari = AutoRegressionInputs(base_directory)
    assert ari.base_directory == base_directory


def test_get_available_parameters(test_data_dir):
    """Should get 12 jobs"""
    base_directory = os.path.join(test_data_dir, "demo")
    ari = AutoRegressionInputs(base_directory)
    ari.get_available_parameters()
    assert len(ari._parameters) == 3

    param = AutoRegressionParameters(country="United States", data="data.csv")
    key = param.name

    assert key in ari._parameters


def test_iter_jobs(test_data_dir):
    """Should return a list of jobs"""
    base_directory = os.path.join(test_data_dir, "demo")
    ari = AutoRegressionInputs(base_directory)
    ari.get_available_parameters()

    jobs = ari.iter_jobs()
    assert len(jobs) == 3
    assert isinstance(jobs[0], AutoRegressionParameters)
