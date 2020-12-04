
import os
import pytest


@pytest.fixture
def test_data_dir():
    """The path to the directory that contains the fixture data"""
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def example_output():
    return os.path.join(os.path.dirname(__file__), "data", "example_output")
