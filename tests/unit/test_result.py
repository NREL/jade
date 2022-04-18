import mock
import pytest
from jade.result import *


@pytest.fixture
def jade_results():
    """Fixture of jade results"""
    return [
        Result("deployment__result__1", 0, "finished", 10, 15555555555, "1"),
        Result("deployment__result__2", 1, "finished", 20, 15555555555, "2"),
        Result("deployment__result__3", 0, "finished", 30, 15555555555, "3"),
    ]


@pytest.fixture
def jade_data():
    """Fixture of serialized jade result"""
    return {
        "base_directory": "/jade/results/base/directory/",
        "results": [
            {
                "name": "deployment__result__1",
                "return_code": 0,
                "status": "finished",
                "exec_time_s": 10,
                "completion_time": 15555555555,
                "hpc_job_id": "1",
            },
            {
                "name": "deployment__result__2",
                "return_code": 1,
                "status": "finished",
                "exec_time_s": 20,
                "completion_time": 15555555555,
                "hpc_job_id": "2",
            },
            {
                "name": "deployment__result__3",
                "return_code": 0,
                "status": "finished",
                "exec_time_s": 30,
                "completion_time": 15555555555,
                "hpc_job_id": "3",
            },
        ],
        "jade_version": 0.1,
        "timestamp": "2019-09-02 15:00:00",
    }


def test_serialize_result(jade_results, jade_data):
    """Should return dict result as expected"""
    i = 0
    data = serialize_result(jade_results[i])
    assert data == jade_data["results"][i]


def test_serialize_results(jade_results, jade_data):
    """Should return a list of dict result as expected"""
    data = serialize_results(jade_results)
    assert data == jade_data["results"]


def test_deserialize_result(jade_data, jade_results):
    """Should return the jade result as expected"""
    i = 2
    result = deserialize_result(jade_data["results"][i])
    assert result == jade_results[i]


def test_deserialize_results(jade_data, jade_results):
    """Should result a list of jade result as expected"""
    results = deserialize_results(jade_data["results"])
    assert list(results.values()) == jade_results


# Disabled per DISCO-205
# @pytest.fixture
# def results_summary(jade_data):
#    """Fixture of ResultsSummary instance"""
#    ResultsSummary._parse = mock.MagicMock(return_value=jade_data)
#    rs = ResultsSummary(output_dir="/jade/output/dir/")
#    return rs


# def test_results_summary__base_directory(results_summary):
#    """Expected base directory defined in fixture"""
#    assert results_summary.base_directory == "/jade/results/base/directory/"
#
#
# def test_results_summary__results(results_summary):
#    """Expected deserialized deserialized"""
#    results = results_summary.results["results"]
#    assert "base_directory" in results_summary.results
#    assert len(results) == 3
#    assert isinstance(results[0], Result)
#
#
# def _parse(*args, **kwargs):
#    """New callable for ResultsSummary._parse"""
#    return "data"
#
#
# @mock.patch.object(ResultsSummary, "_parse", _parse)
# def test_results_summary___parse():
#    """Expected _parse method to be called"""
#    results_file = "/jade/output/dir/results.tmol"
#    data = ResultsSummary._parse(results_file)
#    assert data == "data"
#
#
# def test_results_summary__get_successful_result(results_summary):
#    """Get expected result by giving job_name"""
#    test_job_name = "deployment__result__1"
#    result = results_summary.get_successful_result(test_job_name)
#    assert isinstance(result, Result)
#    assert result.return_code == 0
#    assert result.status == "finished"
#
#
# def test_results_summary__get_successful_result__unfinished(results_summary):
#    """Get expected result by giving job_name"""
#    test_job_name = "deployment__result__2"
#
#    from jade.exceptions import ExecutionError
#    with pytest.raises(ExecutionError) as exc:
#        result = results_summary.get_successful_result(test_job_name)
#
#        assert "result wasn't successful:".format(result) in str(exc.value)
#
#
# def test_results_summary__get_successful_result__invalid(results_summary):
#    """Get expected result by giving job_name"""
#    test_job_name = "deployment__result__4"
#
#    from jade.exceptions import InvalidParameter
#    with pytest.raises(InvalidParameter) as exc:
#        results_summary.get_successful_result(test_job_name)
#
#    assert "result not found {}".format(test_job_name) in str(exc.value)
#
# def test_results_summary__get_successful_results(results_summary):
#    """Get expected length 2 list with successful results"""
#    successful_results = results_summary.get_successful_results()
#    assert isinstance(successful_results, list)
#    assert len(successful_results) == 2
#
# def test_results_summary__get_failed_results(results_summary):
#    """Get expected length 1 list with failed results"""
#    failed_results = results_summary.get_failed_results()
#    assert isinstance(failed_results, list)
#    assert len(failed_results) == 1
#
# def test_results_summary__list_results(results_summary):
#    """Should return a list of instanes of Result"""
#    results = results_summary.list_results()
#    assert len(results) == 3
#    assert isinstance(results[0], Result)
#
#
# def test_results_summary__show_results(results_summary, capsys):
#    """Should print results on standard output"""
#
#    results_summary.show_results()
#    captured = capsys.readouterr()
#    assert "Max execution time (s)" in captured.out
#
#    results_summary.show_results(only_failed=True)
#    captured = capsys.readouterr()
#    assert "deployment__result__2" in captured.out
#    assert "deployment__result__1" not in captured.out
