from concurrent.futures import ProcessPoolExecutor
import os
import shutil
import tempfile

import pytest

from jade.common import get_temp_results_filename
from jade.jobs.results_aggregator import ResultsAggregator
from jade.result import Result


OUTPUT = os.path.join(tempfile.gettempdir(), "results-aggregator-output")


@pytest.fixture
def cleanup():
    pytest.aggregator = None
    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)
    yield
    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)


def create_result(index):
    """Creates a result with unique fields based on an index."""
    return Result(str(index), index, "finished", 1.0 + index)


def append(result):
    if int(result.name) % 2 == 0:
        pytest.aggregator.append_result(result)


def test_results_aggregator(cleanup):
    """Test ResultsAggregator"""
    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)

    results = [create_result(i) for i in range(100)]
    os.makedirs(OUTPUT)
    pytest.aggregator = ResultsAggregator.create(OUTPUT)
    assert os.path.exists(pytest.aggregator._processed_filename)

    with ProcessPoolExecutor() as executor:
        executor.map(append, results)

    final_results = pytest.aggregator.process_results()
    final_results.sort(key=lambda x: int(x.name))

    expected = [x for x in results if int(x.name) % 2 == 0]

    assert final_results == expected
