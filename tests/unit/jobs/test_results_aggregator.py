
from concurrent.futures import ProcessPoolExecutor
import os
import shutil
import tempfile

import pytest

from jade.common import get_results_temp_filename, RESULTS_DIR
from jade.jobs.results_aggregator import ResultsAggregator, \
    ResultsAggregatorSummary
from jade.result import Result


OUTPUT = os.path.join(tempfile.gettempdir(), "results-aggregator-output")


@pytest.fixture
def cleanup():
    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)


def create_result(index):
    """Creates a result with unique fields based on an index."""
    return Result(str(index), index, "finished", 1.0 + index)


aggregator1 = None
aggregator2 = None


def append(result):
    if int(result.name) % 2 == 0:
        aggregator1.append_result(result)
    else:
        aggregator2.append_result(result)


def test_results_aggregator(cleanup):
    """Test ResultsAggregator"""
    global aggregator1, aggregator2
    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)
    os.makedirs(os.path.join(OUTPUT, RESULTS_DIR))

    results = [create_result(i) for i in range(100)]
    batch1_file = get_results_temp_filename(OUTPUT, 1)
    batch2_file = get_results_temp_filename(OUTPUT, 2)
    aggregator1 = ResultsAggregator(batch1_file)
    aggregator2 = ResultsAggregator(batch2_file)
    aggregator1.create_file()
    aggregator2.create_file()
    assert os.path.exists(aggregator1._filename)
    assert os.path.exists(aggregator2._filename)

    with ProcessPoolExecutor() as executor:
        executor.map(append, results)

    final_results1 = aggregator1.get_results()
    final_results1.sort(key=lambda x: int(x.name))
    final_results2 = aggregator2.get_results()
    final_results2.sort(key=lambda x: int(x.name))

    expected1 = [x for x in results if int(x.name) % 2 == 0]
    expected2 = [x for x in results if int(x.name) % 2 != 0]

    assert final_results1 == expected1
    assert final_results2 == expected2

    results_dir = os.path.join(OUTPUT, RESULTS_DIR)
    summary = ResultsAggregatorSummary(results_dir)
    final_results = summary.get_results()
    final_results.sort(key=lambda x: int(x.name))
    assert final_results == results

    summary.delete_files()
    assert not os.listdir(results_dir)
