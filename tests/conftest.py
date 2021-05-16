import os
import sys

import pytest


if os.environ.get("LOCAL_SUBMITTER") is not None:
    print("You must unset the environment variable LOCAL_SUBMITTER.")
    sys.exit(1)


@pytest.fixture
def test_data_dir():
    """The path to the directory that contains the fixture data"""
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def example_output():
    return os.path.join(os.path.dirname(__file__), "data", "example_output")
